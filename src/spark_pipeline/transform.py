from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from cleaning.records import ENTITY_ORDER
from pipeline.validation import ID_FIELDS, RELATIONSHIPS
from spark_pipeline.decoding import decode_message
from spark_pipeline.schemas import DECODED_SCHEMA, entity_schema

AUDIT_FIELDS = ("key", "value", "topic", "partition", "offset", "timestamp", "entity", "source_system", "reason")


class Snapshot:
    def __init__(self, raw: DataFrame, prefix: str):
        self.cached: list[DataFrame] = []
        self.datasets: dict[str, DataFrame] = {}
        self.rejected: DataFrame

        self.raw_count = raw.count()

        try:
            self._build(raw, prefix)
        except BaseException:
            self.close()
            raise

    def cache(self, frame: DataFrame) -> DataFrame:
        frame.persist(StorageLevel.MEMORY_AND_DISK)

        self.cached.append(frame)

        return frame

    def _build(self, raw: DataFrame, prefix: str) -> None:
        decode = F.udf(lambda value, key, topic: decode_message(value, key, topic, prefix), DECODED_SCHEMA)

        decoded = self.cache(raw.withColumn("decoded", decode("value", "key", "topic")).select("*", "decoded.*"))

        rejected = [decoded.filter(F.col("reason").isNotNull()).select(*AUDIT_FIELDS)]

        for entity in ENTITY_ORDER:
            identifier = ID_FIELDS[entity]

            candidates = decoded.filter((F.col("entity") == entity) & F.col("reason").isNull())

            candidates = candidates.withColumn(
                "record", F.from_json("mapped_json", entity_schema(entity), {"mode": "FAILFAST"})
            )

            if entity in RELATIONSHIPS:
                parent, foreign_key = RELATIONSHIPS[entity]

                parents = self.datasets[parent].select(
                    F.col(ID_FIELDS[parent]).alias("parent_id"), F.col("source_system").alias("parent_source")
                )

                joined = candidates.join(parents, F.col(f"record.{foreign_key}") == F.col("parent_id"), "left")

                reason = F.when(F.col("parent_id").isNull(), F.lit(f"Orphan {foreign_key} in {entity}")).when(
                    F.col("parent_source") != F.col("source_system"), F.lit(f"Cross-source {foreign_key} in {entity}")
                )

                joined = self.cache(joined.withColumn("reason", reason))

                rejected.append(joined.filter(F.col("reason").isNotNull()).select(*AUDIT_FIELDS))

                candidates = joined.filter(F.col("reason").isNull())

            order = Window.partitionBy(F.col(f"record.{identifier}")).orderBy("topic", "partition", "offset")

            ranked = self.cache(candidates.withColumn("rank", F.row_number().over(order)))

            duplicates = ranked.filter(F.col("rank") > 1).withColumn(
                "reason", F.concat(F.lit(f"Duplicate identifier in {entity}: "), F.col(f"record.{identifier}"))
            )

            rejected.append(duplicates.select(*AUDIT_FIELDS))

            unique = ranked.filter(F.col("rank") == 1)

            provenance = F.struct(*[F.col(name) for name in ("topic", "partition", "offset", "timestamp", "key")])

            self.datasets[entity] = self.cache(unique.select("record.*", provenance.alias("_kafka")))

        self.rejected = self.cache(reduce(DataFrame.unionByName, rejected))

    def counts(self) -> dict:
        accepted = {entity: frame.count() for entity, frame in self.datasets.items()}

        rejected = self.rejected.count()

        duplicates = self.rejected.filter(F.col("reason").startswith("Duplicate identifier")).count()

        if self.raw_count != sum(accepted.values()) + rejected:
            raise RuntimeError("Spark accounting failed: input != accepted + rejected")

        return {
            "input": self.raw_count,
            "accepted": accepted,
            "rejected": rejected,
            "duplicates": duplicates,
            "invalid": rejected - duplicates,
        }

    def close(self) -> None:
        for frame in reversed(self.cached):
            frame.unpersist()

        self.cached.clear()
