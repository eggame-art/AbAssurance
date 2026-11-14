from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from dataset_preparation.config import FEATURE_COLUMNS, PreparationConfig
from dataset_preparation.features import PreparedDataset
from models.history import HISTORY_IDS
from pipeline.validation import ID_FIELDS
from shared.errors import ConfigurationError
from spark_pipeline.schemas import entity_schema


def latest_before(frame: DataFrame, entity: str, cutoff: str) -> DataFrame:
    key = HISTORY_IDS[entity]
    order = Window.partitionBy(key, "source_system").orderBy(F.col("revision").desc())

    return (
        frame.filter(F.try_to_timestamp("available_at") < F.lit(cutoff).cast("timestamp"))
        .withColumn("_rank", F.row_number().over(order))
        .filter("_rank = 1 AND NOT deleted")
        .drop("_rank")
    )


class HistoricalDataset:
    def __init__(self, frames: dict[str, DataFrame], config: PreparationConfig, manifest: dict):
        self.cached: list[DataFrame] = []
        self.audits: list[DataFrame] = []
        self.base: PreparedDataset | None = None

        self.config = config
        self.manifest = manifest

        try:
            self._build(frames)
        except BaseException:
            self.close()
            raise

    def cache(self, frame: DataFrame) -> DataFrame:
        self.cached.append(frame.persist(StorageLevel.MEMORY_AND_DISK))

        return frame

    def select(self, frame: DataFrame, entity: str, cutoff: str) -> DataFrame:
        key = HISTORY_IDS[entity]
        eligible = F.try_to_timestamp("available_at") < F.lit(cutoff).cast("timestamp")
        order = Window.partitionBy(key, "source_system").orderBy(eligible.desc(), F.col("revision").desc())

        checked = self.cache(
            frame.withColumn("_rank", F.row_number().over(order)).withColumn(
                "_reason",
                F.when(~eligible, "not_yet_available")
                .when(F.col("_rank") > 1, "superseded_version")
                .when(F.col("deleted"), "deleted_version"),
            )
        )

        self.audits.append(
            checked.filter("_reason IS NOT NULL").select(
                F.lit(entity).alias("entity"),
                F.col(key).alias("record_id"),
                "source_system",
                F.col("_reason").alias("reason"),
            )
        )

        return self.cache(checked.filter("_reason IS NULL").drop("_reason", "_rank"))

    def _build(self, frames: dict[str, DataFrame]) -> None:
        t = self.config.observation_date
        freeze = self.manifest["as_of_date"]
        keys = ["contract_id", "source_system"]

        selected = {entity: self.select(frames[entity], entity, t) for entity in ID_FIELDS}

        unified = {
            entity: frame.select(
                *[
                    F.col(field.name)
                    if field.name in frame.columns
                    else F.lit(None).cast(field.dataType).alias(field.name)
                    for field in entity_schema(entity)
                ]
            )
            for entity, frame in selected.items()
        }

        self.base = PreparedDataset(unified, t)

        followup = self.select(frames["followup"], "followup", freeze).select(
            *keys, "complete_from", "complete_until", "features_complete_from"
        )

        outcomes = self.select(frames["cancellations"], "cancellations", freeze)
        occurred = outcomes.filter(F.col("cancellation_date") < F.lit(freeze))

        first_dates = occurred.groupBy(*keys).agg(F.min("cancellation_date").alias("first_exit_date"))

        first_exits = occurred.join(first_dates, keys).filter(F.col("cancellation_date") == F.col("first_exit_date"))

        first_exits = first_exits.groupBy(*keys).agg(
            F.min("cancellation_date").alias("first_exit_date"),
            F.min("cancellation_type").alias("exit_type"),
            F.countDistinct("cancellation_type").alias("exit_type_count"),
        )

        announced = (
            latest_before(frames["cancellations"], "cancellations", t)
            .filter(F.col("cancellation_date") >= F.lit(t))
            .select(*keys)
            .distinct()
            .withColumn("_announced", F.lit(True))
        )

        truth = latest_before(frames["contracts"], "contracts", freeze).select(
            *keys, F.col("start_date").alias("_truth_start"), F.col("coverage_end_date").alias("_truth_end")
        )

        candidates = (
            self.base.dataset.drop("is_cancelled", "training_eligible")
            .join(selected["contracts"].select(*keys, "start_date", "coverage_end_date"), keys)
            .join(truth, keys, "left")
            .join(followup, keys, "left")
            .join(first_exits, keys, "left")
            .join(announced, keys, "left")
            .withColumn("horizon_end", F.date_add(F.lit(t), self.config.horizon_days))
        )

        cohort_reason = (
            F.when(F.col("_truth_start").isNull(), "contract_removed_from_truth")
            .when(F.col("_truth_start") >= F.lit(t), "contract_not_started_in_truth")
            .when((F.col("coverage_end_date") <= F.lit(t)) | (F.col("_truth_end") <= F.lit(t)), "coverage_ended")
            .when(F.col("first_exit_date") < F.lit(t), "exit_before_observation")
            .when(F.col("_announced"), "exit_already_announced")
        )

        candidates = self.cache(candidates.withColumn("_cohort_reason", cohort_reason))

        self.cohort_excluded = self.cache(
            candidates.filter("_cohort_reason IS NOT NULL").select(
                F.lit("cohort").alias("entity"),
                F.col("contract_id").alias("record_id"),
                "source_system",
                F.col("_cohort_reason").alias("reason"),
            )
        )

        rows = candidates.filter("_cohort_reason IS NULL")

        risk_known = F.coalesce(
            (F.col("complete_from") <= F.col("_truth_start")) & (F.col("complete_until") >= F.lit(t)), F.lit(False)
        )

        exit_in_horizon = F.coalesce(F.col("first_exit_date") < F.col("horizon_end"), F.lit(False))

        exit_confirmed = (
            exit_in_horizon
            & (F.col("first_exit_date") < F.col("complete_until"))
            & (F.col("_truth_end").isNull() | (F.col("first_exit_date") <= F.col("_truth_end")))
        )

        label_status = (
            F.when(~risk_known, "insufficient_history_at_observation")
            .when(exit_in_horizon & (F.col("exit_type_count") > 1), "ambiguous_exit")
            .when(exit_confirmed & F.col("exit_type").isin(*self.config.cancellation_types), "confirmed_positive")
            .when(exit_confirmed, "competing_exit")
            .when(F.col("_truth_end") < F.col("horizon_end"), "coverage_ends_before_horizon")
            .when(
                (F.col("complete_until") >= F.col("horizon_end"))
                & (F.lit(freeze).cast("date") >= F.col("horizon_end"))
                & ~exit_in_horizon,
                "confirmed_negative",
            )
            .otherwise("insufficient_followup")
        )

        rows = rows.withColumn("label_status", label_status).withColumn(
            "is_cancelled",
            F.when(F.col("label_status") == "confirmed_positive", 1)
            .when(F.col("label_status") == "confirmed_negative", 0)
            .cast("integer"),
        )

        feature_history_known = F.coalesce(F.col("features_complete_from") <= F.col("start_date"), F.lit(False))

        rows = rows.withColumn("training_eligible", F.col("is_cancelled").isNotNull() & feature_history_known)

        rows = rows.withColumn(
            "eligibility_reason",
            F.when(F.col("is_cancelled").isNull(), F.col("label_status"))
            .when(~feature_history_known, "feature_history_incomplete")
            .otherwise("eligible"),
        )

        self.dataset = self.cache(
            rows.select(
                "contract_id",
                "customer_id",
                "source_system",
                "observation_date",
                "horizon_end",
                *FEATURE_COLUMNS,
                "is_cancelled",
                "training_eligible",
                "label_status",
                "eligibility_reason",
                F.lit(self.manifest["is_synthetic"]).alias("is_synthetic"),
            )
        )

        self.excluded = self.cache(
            reduce(DataFrame.unionByName, [self.base.excluded, *self.audits, self.cohort_excluded])
        )

    def report(self) -> dict:
        base = self.base

        if base is None:
            raise RuntimeError("Historical dataset is not initialized")

        report = base.report()

        if any(row["reason"] == "event_before_contract_start" for row in report["excluded"]):
            raise ConfigurationError("Historical event precedes its point-in-time contract start")

        report["feature_input"] = report.pop("input")
        report["feature_accepted"] = report.pop("accepted")
        report["feature_exclusions"] = report.pop("excluded")

        report["cohort_candidates"] = report["records"]
        report["cohort_excluded_records"] = self.cohort_excluded.count()
        report["records"] = self.dataset.count()
        report["unknown_targets"] = self.dataset.filter("is_cancelled IS NULL").count()
        report["training_eligible_records"] = self.dataset.filter("training_eligible").count()

        report["class_counts"] = {
            str(row.is_cancelled): row["count"]
            for row in self.dataset.filter("training_eligible").groupBy("is_cancelled").count().collect()
        }

        report["label_status"] = {
            row.label_status: row["count"] for row in self.dataset.groupBy("label_status").count().collect()
        }

        report["excluded"] = sorted(
            [row.asDict() for row in self.excluded.groupBy("entity", "reason").count().collect()],
            key=lambda row: (row["entity"], row["reason"]),
        )

        missing = self.dataset.agg(
            *[F.count(F.when(F.col(name).isNull(), 1)).alias(name) for name in FEATURE_COLUMNS]
        ).first()

        if missing is None:
            raise RuntimeError("Historical feature summary is unavailable")

        report["missing_features"] = missing.asDict()

        if (
            report["records"] + report["cohort_excluded_records"] != report["cohort_candidates"]
            or self.dataset.select("contract_id").distinct().count() != report["records"]
        ):
            raise RuntimeError("Historical cohort accounting failed")

        return report

    def close(self) -> None:
        for frame in reversed(self.cached):
            frame.unpersist()

        self.cached.clear()

        if self.base is not None:
            self.base.close()
