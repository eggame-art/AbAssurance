from dataclasses import replace

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from abassurance.systems.data_preparation.config import FEATURE_COLUMNS, PreparationConfig
from abassurance.systems.data_preparation.split import assign_split, split_summary, validate_split
from abassurance.systems.data_preparation.temporal import HistoricalDataset
from abassurance.systems.spark_pipeline.hdfs import HdfsState


class PreparedCohorts:
    def __init__(
        self,
        spark: SparkSession,
        frames: dict[str, DataFrame],
        config: PreparationConfig,
        manifest: dict,
        output: HdfsState,
        attempt: str,
    ):
        self.cached: list[DataFrame] = []
        self.cohorts: list[dict] = []
        self.config = config
        self.work_folder = f"{attempt}/_cohorts"

        try:
            paths, audit_paths = [], []

            for day in config.dates:
                single = replace(config, observation_date=day, observation_dates=(), evaluation_start=None)

                label_as_of = manifest["as_of_date"]

                if config.evaluation_start and day < config.evaluation_start:
                    label_as_of = min(label_as_of, config.evaluation_start)

                cohort = HistoricalDataset(frames, single, {**manifest, "as_of_date": label_as_of})

                try:
                    self.cohorts.append(
                        {"observation_date": day, "label_as_of": label_as_of, "counts": cohort.report()}
                    )

                    folder = f"{self.work_folder}/{day}"
                    path = output.path(f"{folder}/data")
                    audit = output.path(f"{folder}/excluded")

                    cohort.dataset.write.mode("errorifexists").parquet(path)

                    cohort.excluded.withColumn("observation_date", F.lit(day).cast("date")).write.mode(
                        "errorifexists"
                    ).parquet(audit)

                    paths.append(path)
                    audit_paths.append(audit)
                finally:
                    cohort.close()

            self.dataset = self.cache(assign_split(spark.read.parquet(*paths), config))

            self.checks = validate_split(self.dataset, config)

            split_audit = self.dataset.filter("training_eligible AND dataset_split = 'excluded'").select(
                F.lit("split").alias("entity"),
                F.col("contract_id").alias("record_id"),
                "source_system",
                F.col("split_reason").alias("reason"),
                "observation_date",
            )

            split_path = output.path(f"{self.work_folder}/split-excluded")
            split_audit.write.mode("errorifexists").parquet(split_path)

            self.excluded = self.cache(spark.read.parquet(*audit_paths, split_path))
        except BaseException:
            self.close()
            raise

    def cache(self, frame: DataFrame) -> DataFrame:
        self.cached.append(frame.persist(StorageLevel.MEMORY_AND_DISK))

        return frame

    def report(self) -> dict:
        summary = split_summary(self.dataset, self.config)

        report = {
            "records": self.dataset.count(),
            "cohorts": self.cohorts,
            "unknown_targets": self.dataset.filter("is_cancelled IS NULL").count(),
            "training_eligible_records": self.dataset.filter("training_eligible").count(),
            "split_excluded_eligible_records": self.dataset.filter(
                "training_eligible AND dataset_split = 'excluded'"
            ).count(),
            "class_counts": {
                str(row.is_cancelled): row["count"]
                for row in self.dataset.filter("training_eligible").groupBy("is_cancelled").count().collect()
            },
            "splits": summary,
            "excluded": sorted(
                [row.asDict() for row in self.excluded.groupBy("entity", "reason").count().collect()],
                key=lambda row: (row["entity"], row["reason"]),
            ),
        }

        missing = self.dataset.agg(
            *[F.count(F.when(F.col(name).isNull(), 1)).alias(name) for name in FEATURE_COLUMNS]
        ).first()

        if missing is None:
            raise RuntimeError("Cohort feature summary is unavailable")

        report["missing_features"] = missing.asDict()

        return report

    def close(self) -> None:
        for frame in reversed(self.cached):
            frame.unpersist()

        self.cached.clear()
