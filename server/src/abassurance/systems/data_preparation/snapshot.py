import re
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_integration.pipeline.validation import ID_FIELDS
from abassurance.systems.spark_pipeline.hdfs import HdfsState, read_snapshot
from abassurance.systems.spark_pipeline.schemas import entity_schema


def load_current(spark: SparkSession, state: HdfsState) -> tuple[dict[str, Any], dict[str, DataFrame]]:
    manifest = state.read_json("CURRENT.json")
    identifier = manifest.get("snapshot")

    if (
        not isinstance(identifier, str)
        or not re.fullmatch(r"[0-9a-f]{32}", identifier)
        or manifest.get("format") != "parquet"
    ):
        raise ConfigurationError("Invalid HDFS snapshot manifest")

    folder = f"snapshots/{identifier}"

    if state.read_json(f"{folder}/report.json") != manifest:
        raise ConfigurationError("CURRENT does not match the source snapshot report")

    accepted = manifest.get("counts", {}).get("accepted", {})
    frames = {}

    for entity in (*ID_FIELDS, "rejected"):
        path = "rejected" if entity == "rejected" else f"unified/{entity}"

        if not state.exists(f"{folder}/{path}/_SUCCESS"):
            raise ConfigurationError(f"Incomplete source Parquet: {entity}")

        if entity == "rejected":
            continue

        if type(accepted.get(entity)) is not int or accepted[entity] < 0:
            raise ConfigurationError(f"Invalid source count: {entity}")

        frame = read_snapshot(spark, state.root, entity, manifest=manifest)
        actual = {field.name: field.dataType for field in frame.schema}

        if any(actual.get(field.name) != field.dataType for field in entity_schema(entity)):
            raise ConfigurationError(f"Incompatible unified schema: {entity}")

        frames[entity] = frame.select(*entity_schema(entity).fieldNames())

    return manifest, frames
