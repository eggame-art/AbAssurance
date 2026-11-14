from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from dataset_preparation.config import FEATURE_COLUMNS
from mapping.identifiers import SOURCE_PREFIXES
from pipeline.validation import ID_FIELDS, RELATIONSHIPS


class PreparedDataset:
    def __init__(self, frames: dict[str, DataFrame], observation_date: str):
        self.cached: list[DataFrame] = []
        self.accepted: dict[str, DataFrame] = {}
        self.audits: list[DataFrame] = []
        self.input_counts: dict[str, int] = {}
        self.observation_date = observation_date

        try:
            for entity in ID_FIELDS:
                self._prepare(entity, frames[entity])

            self.excluded = self.cache(reduce(DataFrame.unionByName, self.audits))
            self.dataset = self.cache(self._features())
        except BaseException:
            self.close()
            raise

    def cache(self, frame: DataFrame) -> DataFrame:
        self.cached.append(frame.persist(StorageLevel.MEMORY_AND_DISK))

        return frame

    def _prepare(self, entity: str, frame: DataFrame) -> None:
        identifier = ID_FIELDS[entity]
        self.input_counts[entity] = frame.count()

        prefix = F.create_map(*[F.lit(value) for pair in SOURCE_PREFIXES.items() for value in pair])

        reference = F.col("source_reference" if entity == "contracts" else "source_id")

        valid_identity = (F.length(F.trim(reference)) > 0) & (
            F.col(identifier) == F.concat(prefix[F.col("source_system")], F.lit("-"), reference)
        )

        duplicates = F.count(F.lit(1)).over(Window.partitionBy(identifier))

        frame = frame.withColumn(
            "_reason",
            F.when(~F.coalesce(valid_identity, F.lit(False)), "invalid_identity").when(
                duplicates > 1, "duplicate_identifier"
            ),
        )

        if entity in RELATIONSHIPS:
            parent, key = RELATIONSHIPS[entity]
            parent_columns = [key, "source_system"]

            if parent == "contracts":
                parent_columns.append("_start")

            parents = self.accepted[parent].select(*parent_columns).withColumn("_parent", F.lit(True))

            frame = (
                frame.join(parents, [key, "source_system"], "left")
                .withColumn(
                    "_reason", F.coalesce("_reason", F.when(F.col("_parent").isNull(), "orphan_or_cross_source_parent"))
                )
                .drop("_parent")
            )

        if entity == "contracts":
            frame = frame.withColumn("_start", F.try_to_timestamp("start_date").cast("date"))

            frame = frame.withColumn(
                "_reason",
                F.coalesce(
                    "_reason",
                    F.when(
                        F.col("_start").isNull() | ~F.col("start_date").rlike(r"^\d{4}-\d{2}-\d{2}$"),
                        "missing_or_invalid_start_date",
                    ).when(F.col("_start") >= F.lit(self.observation_date).cast("date"), "contract_not_started"),
                ),
            )

        if entity in ("claims", "payments"):
            field = "incident_date" if entity == "claims" else "payment_date"

            frame = frame.withColumn("_event_date", F.try_to_timestamp(F.col(field)).cast("date"))

            frame = frame.withColumn(
                "_reason",
                F.coalesce(
                    "_reason",
                    F.when(F.col("_event_date").isNull(), "missing_or_invalid_event_date")
                    .when(F.col("_event_date") < F.col("_start"), "event_before_contract_start")
                    .when(
                        F.col("_event_date") >= F.lit(self.observation_date).cast("date"),
                        "event_on_or_after_observation",
                    ),
                ),
            )

        if entity == "payments":
            frame = frame.withColumn("_amount", F.expr("try_cast(amount as decimal(28,2))"))

            frame = frame.withColumn(
                "_reason",
                F.coalesce(
                    "_reason",
                    F.when(
                        F.col("amount").isNotNull() & (F.col("_amount").isNull() | (F.col("_amount") < 0)),
                        "invalid_payment_amount",
                    ),
                ),
            )

        checked = self.cache(frame)

        self.audits.append(
            checked.filter(F.col("_reason").isNotNull()).select(
                F.lit(entity).alias("entity"),
                F.col(identifier).alias("record_id"),
                "source_system",
                F.col("_reason").alias("reason"),
            )
        )

        self.accepted[entity] = self.cache(checked.filter(F.col("_reason").isNull()).drop("_reason"))

    def _features(self) -> DataFrame:
        keys = ["contract_id", "source_system"]

        contracts = self.accepted["contracts"].select(*keys, "customer_id", "_start")

        claims = self.accepted["claims"].groupBy(*keys).agg(F.count("claim_id").alias("claim_count"))

        payments = (
            self.accepted["payments"]
            .groupBy(*keys)
            .agg(
                F.count("payment_id").alias("payment_count"),
                F.sum("_amount").alias("payment_amount_known_total"),
                F.sum(F.col("_amount").isNull().cast("long")).alias("payment_amount_missing_count"),
                F.max("_event_date").alias("_last_payment"),
            )
        )

        result = contracts.join(claims, keys, "left").join(payments, keys, "left")

        result = result.fillna(0, subset=["claim_count", "payment_count", "payment_amount_missing_count"])

        result = result.withColumn(
            "payment_amount_known_total",
            F.when(F.col("payment_count") == 0, F.lit(0).cast("decimal(38,2)")).otherwise(
                F.col("payment_amount_known_total")
            ),
        )

        return result.select(
            "contract_id",
            "customer_id",
            "source_system",
            F.lit(self.observation_date).cast("date").alias("observation_date"),
            F.datediff(F.lit(self.observation_date), "_start").alias("contract_tenure_days"),
            "claim_count",
            "payment_count",
            "payment_amount_known_total",
            "payment_amount_missing_count",
            F.datediff(F.lit(self.observation_date), "_last_payment").alias("days_since_last_payment"),
            F.lit(None).cast("integer").alias("is_cancelled"),
            F.lit(False).alias("training_eligible"),
        )

    def report(self) -> dict:
        accepted = {entity: frame.count() for entity, frame in self.accepted.items()}

        reasons = [row.asDict() for row in self.excluded.groupBy("entity", "reason").count().collect()]

        for entity in ID_FIELDS:
            excluded = sum(row["count"] for row in reasons if row["entity"] == entity)

            if self.input_counts[entity] != accepted[entity] + excluded:
                raise RuntimeError(f"Dataset accounting failed: {entity}")

        count = self.dataset.count()

        if count != accepted["contracts"] or self.dataset.select("contract_id").distinct().count() != count:
            raise RuntimeError("Expected exactly one dataset row per accepted contract")

        missing = self.dataset.agg(
            *[F.count(F.when(F.col(name).isNull(), 1)).alias(name) for name in FEATURE_COLUMNS]
        ).first()

        if missing is None:
            raise RuntimeError("Dataset feature summary is unavailable")

        return {
            "input": self.input_counts,
            "accepted": accepted,
            "excluded": sorted(reasons, key=lambda row: (row["entity"], row["reason"])),
            "records": count,
            "unknown_targets": count,
            "training_eligible_records": 0,
            "missing_features": missing.asDict(),
        }

    def close(self) -> None:
        for frame in reversed(self.cached):
            frame.unpersist()

        self.cached.clear()
