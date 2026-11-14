import argparse
import hashlib
import json
import logging
import os
from typing import TYPE_CHECKING
from uuid import uuid4

from dataset_preparation.config import FEATURE_COLUMNS, VERSION, PreparationConfig
from shared.errors import ConfigurationError
from spark_pipeline.hdfs import HdfsState

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


LOGGER = logging.getLogger(__name__)


def dataset_recipe(config: PreparationConfig, source_root: str, manifest: dict) -> dict:
    return {
        "version": VERSION,
        "source_root": source_root,
        "source_manifest": manifest,
        "observation_date": config.observation_date,
        "purpose": config.purpose,
        "horizon_days": config.horizon_days,
        "cancellation_types": list(config.cancellation_types),
        "observation_dates": list(config.dates),
        "evaluation_start": config.evaluation_start,
        "embargo_days": config.embargo_days,
        "evaluation_confidence": config.evaluation_confidence,
        "evaluation_margin": config.evaluation_margin,
        "split_policy": "purged-chronological-evaluation-priority-customer-and-contract-v1",
        "training_label_availability_cutoff": config.evaluation_start,
    }


def prepare_dataset(spark: "SparkSession", config: PreparationConfig) -> dict:
    from pyspark.sql import functions as F

    from dataset_preparation.cohorts import PreparedCohorts
    from dataset_preparation.features import PreparedDataset
    from dataset_preparation.snapshot import load_current
    from dataset_preparation.split import assign_split, readiness, split_summary, validate_split

    config.require_supported_purpose()

    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ConfigurationError("Dataset preparation requires spark.sql.session.timeZone=UTC")

    historical = config.purpose != "technical-validation"
    source_root = config.history_root if historical else config.source_root

    if source_root is None:
        raise ConfigurationError("Historical dataset preparation requires a history root")

    source = HdfsState(spark, source_root)

    if historical:
        from dataset_preparation.history import load_history, require_namespace

        manifest, frames = load_history(spark, source)
        synthetic = manifest["is_synthetic"]

        if synthetic != (config.purpose == "synthetic-training"):
            raise ConfigurationError(
                "Use synthetic-training only for synthetic history; training requires business history"
            )

        require_namespace(config.output_root, synthetic)

        if max(config.dates) >= manifest["as_of_date"]:
            raise ConfigurationError("Observation must precede the frozen history as-of-date")
    else:
        manifest, frames = load_current(spark, source)

    recipe = dataset_recipe(config, source.root, manifest)
    dataset_id = hashlib.sha256(json.dumps(recipe, sort_keys=True).encode("utf-8")).hexdigest()

    output = HdfsState(spark, config.output_root)
    destination = f"runs/{dataset_id}"

    with output.lock():
        if output.exists(destination):
            report = output.read_json(f"{destination}/report.json")

            if report.get("recipe") != recipe or report.get("dataset_id") != dataset_id:
                raise ConfigurationError("Existing dataset recipe mismatch")

            _verify_output(spark, output, destination, report)
            output.write_json_atomic("CURRENT.json", report)

            return report

        attempt = f"attempts/{uuid4().hex}"
        output.mkdir(attempt)

        prepared: PreparedCohorts | PreparedDataset

        if historical:
            prepared = PreparedCohorts(spark, frames, config, manifest, output, attempt)
        else:
            prepared = PreparedDataset(frames, config.observation_date)

        try:
            if isinstance(prepared, PreparedDataset) and prepared.input_counts != manifest["counts"]["accepted"]:
                raise ConfigurationError("Source Parquet counts do not match snapshot manifest")

            counts = prepared.report()
            dataset = prepared.dataset

            if not historical:
                dataset = assign_split(
                    dataset.withColumn("horizon_end", F.date_add("observation_date", config.horizon_days)), config
                )

                counts["splits"] = split_summary(dataset, config)

            checks = validate_split(dataset, config)

            dataset = dataset.withColumn("source_snapshot", F.lit(manifest["snapshot"]))
            dataset.write.mode("errorifexists").parquet(output.path(f"{attempt}/data"))

            audit = prepared.excluded

            if not historical:
                audit = audit.withColumn("observation_date", F.lit(config.observation_date).cast("date"))

            audit.write.mode("errorifexists").parquet(output.path(f"{attempt}/excluded"))

            for name in ("training", "evaluation"):
                dataset.filter(F.col("dataset_split") == name).write.mode("errorifexists").parquet(
                    output.path(f"{attempt}/{name}")
                )

            report = {
                "dataset_id": dataset_id,
                "format": "parquet",
                "recipe": recipe,
                "data_path": output.path(f"{destination}/data"),
                "excluded_path": output.path(f"{destination}/excluded"),
                "counts": counts,
                "feature_columns": FEATURE_COLUMNS,
                "target": {
                    "column": "is_cancelled",
                    "available": historical and counts["unknown_targets"] < counts["records"],
                    "horizon_days": config.horizon_days,
                    "interval": "[observation_date, observation_date + horizon_days)",
                },
                "is_synthetic": manifest.get("is_synthetic", False),
                **readiness(
                    config, counts["splits"], historical=historical, synthetic=manifest.get("is_synthetic", False)
                ),
                "validation": {**checks, "persisted_parquet_verified": True},
                "temporal_policy": (
                    "Features: latest business version available strictly before observation, "
                    "event date before observation; "
                    "labels: frozen outcome history and certified continuous follow-up"
                )
                if historical
                else "event date < observation_date at 00:00 UTC; availability unverified",
                "schema": dataset.schema.jsonValue(),
            }

            report["training_path"] = output.path(f"{destination}/training")
            report["evaluation_path"] = output.path(f"{destination}/evaluation")

            output.write_json_atomic(f"{attempt}/report.json", report)
            _verify_output(spark, output, attempt, report)

            prepared.close()

            if isinstance(prepared, PreparedCohorts):
                if not output.fs.delete(output.java_path(prepared.work_folder), True):
                    raise OSError("Cannot remove completed cohort scratch files")

            output.mkdir("runs")

            rename = output.jvm.org.apache.hadoop.fs.Options.Rename
            options = output.gateway.new_array(rename, 1)
            options[0] = rename.NONE

            output.context.rename(output.java_path(attempt), output.java_path(destination), options)

            output.write_json_atomic("CURRENT.json", report)

            return report
        finally:
            prepared.close()


def _verify_output(spark: "SparkSession", output: HdfsState, folder: str, report: dict) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    from dataset_preparation.split import OBSERVATION_KEY, readiness, split_summary, validate_split

    for name in ("data", "excluded", "training", "evaluation"):
        if not output.exists(f"{folder}/{name}/_SUCCESS"):
            raise ConfigurationError("Incomplete dataset Parquet output")

    data = spark.read.parquet(output.path(f"{folder}/data"))
    expected_schema = StructType.fromJson(report["schema"])

    if [(field.name, field.dataType) for field in data.schema] != [
        (field.name, field.dataType) for field in expected_schema
    ]:
        raise ConfigurationError("Invalid persisted dataset schema")

    counts = report["counts"]
    recipe = report["recipe"]
    historical = recipe["purpose"] != "technical-validation"

    config = PreparationConfig(
        source_root=recipe["source_root"],
        output_root=output.root,
        observation_date=recipe["observation_date"],
        purpose=recipe["purpose"],
        history_root=recipe["source_root"] if historical else None,
        horizon_days=recipe["horizon_days"],
        cancellation_types=tuple(recipe["cancellation_types"]),
        observation_dates=tuple(recipe["observation_dates"]),
        evaluation_start=recipe["evaluation_start"],
        embargo_days=recipe["embargo_days"],
        evaluation_confidence=recipe["evaluation_confidence"],
        evaluation_margin=recipe["evaluation_margin"],
    )

    invalid_target = (
        (
            (F.col("is_cancelled").isNotNull() & ~F.col("is_cancelled").isin(0, 1))
            | (F.col("training_eligible") & F.col("is_cancelled").isNull())
            | ~F.col("is_synthetic").eqNullSafe(F.lit(report["is_synthetic"]))
        )
        if historical
        else (F.col("is_cancelled").isNotNull() | ~F.col("training_eligible").eqNullSafe(F.lit(False)))
    )

    invalid_row = (
        F.col("contract_id").isNull()
        | F.col("training_eligible").isNull()
        | invalid_target
        | ~F.col("source_snapshot").eqNullSafe(F.lit(recipe["source_manifest"]["snapshot"]))
        | F.col("source_system").isNull()
        | F.col("observation_date").isNull()
        | ~F.col("observation_date").cast("string").isin(*config.dates)
        | ~F.col("horizon_end").eqNullSafe(F.date_add("observation_date", config.horizon_days))
    )

    if (
        data.count() != counts["records"]
        or data.select(*OBSERVATION_KEY).distinct().count() != counts["records"]
        or data.filter(invalid_row).limit(1).count()
    ):
        raise ConfigurationError("Invalid persisted dataset counts or eligibility")

    excluded = spark.read.parquet(output.path(f"{folder}/excluded"))

    reasons = sorted(
        [row.asDict() for row in excluded.groupBy("entity", "reason").count().collect()],
        key=lambda row: (row["entity"], row["reason"]),
    )

    if reasons != counts["excluded"]:
        raise ConfigurationError("Invalid persisted exclusion counts")

    checks = validate_split(data, config)

    if report.get("validation") != {**checks, "persisted_parquet_verified": True}:
        raise ConfigurationError("Invalid persisted validation metadata")

    summary = split_summary(data, config)

    if summary != counts["splits"]:
        raise ConfigurationError("Invalid persisted split counts or periods")

    for name in ("training", "evaluation"):
        subset = spark.read.parquet(output.path(f"{folder}/{name}"))

        if [(field.name, field.dataType) for field in subset.schema] != [
            (field.name, field.dataType) for field in expected_schema
        ]:
            raise ConfigurationError(f"Invalid persisted {name} schema")

        expected = data.filter(F.col("dataset_split") == name)

        if (
            subset.count() != summary[name]["records"]
            or subset.exceptAll(expected).limit(1).count()
            or expected.exceptAll(subset).limit(1).count()
        ):
            raise ConfigurationError(f"Invalid persisted {name} subset")

    expected_readiness = readiness(config, summary, historical=historical, synthetic=report["is_synthetic"])

    if any(report.get(key) != value for key, value in expected_readiness.items()):
        raise ConfigurationError("Invalid persisted readiness or statistical planning metadata")


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare contract cancellation data from a committed HDFS snapshot")

    parser.add_argument("--state-dir", default=os.environ.get("SPARK_STATE_DIR", "hdfs://namenode:9000/abassurance"))

    parser.add_argument(
        "--output-dir", default=os.environ.get("DATASET_OUTPUT_DIR", "hdfs://namenode:9000/abassurance-datasets/churn")
    )

    dates = parser.add_mutually_exclusive_group(required=True)
    dates.add_argument("--observation-date", help="Exclusive cutoff, YYYY-MM-DD at 00:00 UTC")
    dates.add_argument("--observation-dates", nargs="+", help="Historical cutoffs, separated by spaces")

    parser.add_argument(
        "--purpose", choices=("training", "technical-validation", "synthetic-training"), default="training"
    )

    parser.add_argument("--history-dir")
    parser.add_argument("--horizon-days", type=int, default=90)
    parser.add_argument("--cancellation-types", default="voluntary,non_payment,non_renewal")
    parser.add_argument("--evaluation-start", help="Inclusive evaluation boundary, YYYY-MM-DD")
    parser.add_argument("--embargo-days", type=int, default=0)
    parser.add_argument("--evaluation-confidence", type=float, default=0.95)
    parser.add_argument("--evaluation-margin", type=float, default=0.10)
    parser.add_argument("--master", default="local[2]")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    spark = None

    try:
        config = PreparationConfig(
            args.state_dir,
            args.output_dir,
            args.observation_date or min(args.observation_dates),
            args.purpose,
            args.history_dir,
            args.horizon_days,
            tuple(args.cancellation_types.split(",")),
            tuple(args.observation_dates or ()),
            args.evaluation_start,
            args.embargo_days,
            args.evaluation_confidence,
            args.evaluation_margin,
        )

        config.require_supported_purpose()

        from spark_pipeline.run import create_session

        spark = create_session(kafka=False, master=args.master)
        spark.sparkContext.setLogLevel("WARN")

        print(json.dumps(prepare_dataset(spark, config), ensure_ascii=False, indent=2))

        return 0
    except Exception:
        LOGGER.exception("Dataset preparation failed")
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
