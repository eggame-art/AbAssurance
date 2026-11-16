import argparse
import json
import re
from dataclasses import fields
from datetime import date
from typing import get_args, get_type_hints
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from abassurance.shared.errors import ConfigurationError
from abassurance.shared.models.history import EXIT_TYPES, HISTORY_IDS, HISTORY_MODELS
from abassurance.systems.data_integration.mapping.identifiers import matches_identity
from abassurance.systems.spark_pipeline.hdfs import HdfsState

HISTORY_VERSION = 1


def history_schema(entity: str) -> StructType:
    model = HISTORY_MODELS[entity]
    hints = get_type_hints(model)
    types = {bool: BooleanType(), int: LongType(), str: StringType()}

    return StructType(
        [StructField(field.name, types.get(hints[field.name], StringType()), True) for field in fields(model)]
    )


def synthetic_namespace(root: str) -> bool:
    from urllib.parse import urlsplit

    return any(part == "synthetic" or part.endswith("-synthetic") for part in urlsplit(root).path.split("/"))


def require_namespace(root: str, is_synthetic: bool) -> None:
    from urllib.parse import unquote

    if unquote(root) != root:
        raise ConfigurationError("Encoded history paths are not supported")

    if synthetic_namespace(root) != is_synthetic:
        raise ConfigurationError("Synthetic history/datasets require a separate synthetic HDFS namespace")


def validate_history(frames: dict[str, DataFrame], *, is_synthetic: bool, as_of_date: str) -> dict[str, int]:
    if type(is_synthetic) is not bool:
        raise ConfigurationError("History provenance must be an explicit boolean")

    if date.fromisoformat(as_of_date).isoformat() != as_of_date:
        raise ConfigurationError("History as-of-date must use YYYY-MM-DD")

    counts = {}

    for entity, model in HISTORY_MODELS.items():
        frame = frames[entity]

        actual = {field.name: field.dataType for field in frame.schema}
        expected = {field.name: field.dataType for field in history_schema(entity)}

        if actual != expected:
            raise ConfigurationError(f"Incompatible business history schema: {entity}")

        hints = get_type_hints(model)
        invalid = F.lit(False)

        for name, hint in hints.items():
            if type(None) not in get_args(hint):
                invalid = invalid | F.col(name).isNull()

        identifier = HISTORY_IDS[entity]

        if entity != "followup":
            reference = F.col("source_reference" if entity == "contracts" else "source_id")

            identity = F.udf(
                lambda source, ref, key, kind=entity: matches_identity(source, ref, key, kind, allow_legacy=True),
                "boolean",
            )("source_system", reference, identifier)

            invalid = invalid | ~F.coalesce(identity, F.lit(False))

        availability = F.try_to_timestamp("available_at")

        invalid = (
            invalid
            | (F.col("revision") < 1)
            | ~F.col("is_synthetic").eqNullSafe(F.lit(is_synthetic))
            | ~F.col("available_at").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
            | availability.isNull()
            | (availability >= F.lit(as_of_date).cast("timestamp"))
        )

        dates = {
            "contracts": ("start_date", "coverage_end_date"),
            "claims": ("incident_date",),
            "payments": ("payment_date",),
            "cancellations": ("cancellation_date",),
            "followup": ("complete_from", "complete_until", "features_complete_from"),
        }.get(entity, ())

        for name in dates:
            invalid = invalid | (
                F.col(name).isNotNull()
                & (~F.col(name).rlike(r"^\d{4}-\d{2}-\d{2}$") | F.try_to_timestamp(name).isNull())
            )

        if entity == "contracts":
            invalid = invalid | (F.col("coverage_end_date") <= F.col("start_date"))

        if entity == "cancellations":
            invalid = invalid | ~F.col("cancellation_type").isin(*EXIT_TYPES)

        if entity == "payments":
            amount = F.expr("try_cast(amount as decimal(28,2))")
            invalid = invalid | (F.col("amount").isNotNull() & (amount.isNull() | (amount < 0)))

        if entity == "followup":
            invalid = (
                invalid
                | (F.col("complete_from") >= F.col("complete_until"))
                | (F.col("features_complete_from") >= F.col("complete_until"))
                | (F.col("complete_until").cast("date") > availability.cast("date"))
            )

        version_key = [identifier, "source_system", "revision"]
        order = Window.partitionBy(identifier, "source_system").orderBy("revision")

        checked = (
            frame.withColumn("_previous", F.lag("available_at").over(order))
            .withColumn("_expected_revision", F.row_number().over(order))
            .withColumn("_copies", F.count(F.lit(1)).over(Window.partitionBy(*version_key)))
        )

        missing_parent = F.lit(False)

        if entity != "customers":
            parent, key = ("customers", "customer_id") if entity == "contracts" else ("contracts", "contract_id")

            parents = frames[parent].select(key, "source_system").distinct().withColumn("_parent", F.lit(True))

            checked = checked.join(parents, [key, "source_system"], "left")
            missing_parent = F.col("_parent").isNull()

        summary = checked.agg(
            F.count(F.lit(1)).alias("records"),
            F.max(F.coalesce(invalid, F.lit(False)).cast("int")).alias("invalid"),
            F.max((F.col("_copies") > 1).cast("int")).alias("duplicates"),
            F.max(
                (
                    (F.col("available_at") <= F.col("_previous")) | (F.col("revision") != F.col("_expected_revision"))
                ).cast("int")
            ).alias("non_monotonic"),
            F.max(missing_parent.cast("int")).alias("orphan"),
        ).first()

        if summary is None:
            raise RuntimeError(f"Business history summary is unavailable: {entity}")

        if summary.invalid:
            raise ConfigurationError(f"Invalid business history values: {entity}")

        if summary.duplicates:
            raise ConfigurationError(f"Conflicting/duplicate business history version: {entity}")

        if summary.non_monotonic:
            raise ConfigurationError(f"Non-monotonic business availability: {entity}")

        if summary.orphan:
            raise ConfigurationError(f"Orphan/cross-source business history: {entity}")

        counts[entity] = summary.records

    return counts


def load_history(spark: SparkSession, state: HdfsState) -> tuple[dict, dict[str, DataFrame]]:
    manifest = state.read_json("CURRENT.json")
    identifier = manifest.get("snapshot")

    if (
        not isinstance(identifier, str)
        or not re.fullmatch(r"[0-9a-f]{32}", identifier)
        or manifest.get("history_version") != HISTORY_VERSION
        or manifest.get("format") != "parquet"
        or type(manifest.get("is_synthetic")) is not bool
    ):
        raise ConfigurationError("Invalid business history manifest")

    require_namespace(state.root, manifest["is_synthetic"])

    folder = f"snapshots/{identifier}"

    if state.read_json(f"{folder}/report.json") != manifest:
        raise ConfigurationError("Business history CURRENT/report mismatch")

    frames = {}

    for entity in HISTORY_MODELS:
        path = f"{folder}/{entity}"

        if not state.exists(f"{path}/_SUCCESS"):
            raise ConfigurationError(f"Incomplete business history Parquet: {entity}")

        frames[entity] = spark.read.parquet(state.path(path))

    counts = validate_history(frames, is_synthetic=manifest["is_synthetic"], as_of_date=manifest["as_of_date"])

    if counts != manifest.get("counts"):
        raise ConfigurationError("Business history counts mismatch")

    return manifest, frames


def publish_history(
    spark: SparkSession,
    root: str,
    incoming: dict[str, DataFrame],
    *,
    is_synthetic: bool,
    as_of_date: str,
    generation: dict | None = None,
) -> dict:
    state = HdfsState(spark, root)
    require_namespace(state.root, is_synthetic)
    if generation is not None and not is_synthetic:
        raise ConfigurationError("Synthetic generation metadata cannot certify business history")

    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ConfigurationError("Business history requires UTC")

    with state.lock():
        previous = None
        frames = incoming

        if state.exists("CURRENT.json"):
            previous, old = load_history(spark, state)

            if previous["is_synthetic"] != is_synthetic or as_of_date < previous["as_of_date"]:
                raise ConfigurationError("History provenance cannot change or observation completeness go backwards")
            if previous.get("generation") != generation:
                raise ConfigurationError("Synthetic generator configuration changed: use a separate history root")

            frames = {entity: old[entity].unionByName(incoming[entity]).dropDuplicates() for entity in HISTORY_MODELS}

        counts = validate_history(frames, is_synthetic=is_synthetic, as_of_date=as_of_date)

        if previous and counts == previous["counts"] and as_of_date == previous["as_of_date"]:
            return previous

        identifier = uuid4().hex
        folder = f"snapshots/{identifier}"

        for entity, frame in frames.items():
            frame.write.mode("errorifexists").parquet(state.path(f"{folder}/{entity}"))

        manifest = {
            "snapshot": identifier,
            "format": "parquet",
            "history_version": HISTORY_VERSION,
            "as_of_date": as_of_date,
            "is_synthetic": is_synthetic,
            "counts": counts,
            "previous_snapshot": previous["snapshot"] if previous else None,
            **({"generation": generation} if generation is not None else {}),
        }

        state.write_json_atomic(f"{folder}/report.json", manifest)
        state.write_json_atomic("CURRENT.json", manifest)

        return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Append certified unified business history from HDFS Parquet")

    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--history-dir", required=True)
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--synthetic", action="store_true")

    args = parser.parse_args()

    from abassurance.systems.spark_pipeline.run import create_session

    spark = create_session(kafka=False)

    try:
        source = HdfsState(spark, args.input_dir)

        frames = {entity: spark.read.parquet(source.path(entity)) for entity in HISTORY_MODELS}

        print(
            json.dumps(
                publish_history(
                    spark, args.history_dir, frames, is_synthetic=args.synthetic, as_of_date=args.as_of_date
                ),
                indent=2,
            )
        )

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
