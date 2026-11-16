import json
import re

from abassurance.shared.environment import Environment
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_api.schemas import ConsolidatedRecord, ConsolidatedResponse, DataEntity, DataKind
from abassurance.systems.data_integration.extraction.config import PROJECT_ROOT
from abassurance.systems.integration_api.schemas import IntegrationRun
from abassurance.systems.integration_api.service import state_directory


def committed_run(integration_id: str | None = None) -> IntegrationRun:
    paths = [state_directory() / f"{integration_id}.json"] if integration_id else state_directory().glob("*.json")
    runs = []

    for path in paths:
        if not re.fullmatch(r"[0-9a-f]{32}", path.stem) or not path.is_file():
            continue

        run = IntegrationRun.model_validate_json(path.read_text("utf-8"))

        if run.status == "succeeded" and run.snapshot and run.finished_at:
            runs.append(run)

    if not runs:
        raise FileNotFoundError("No completed integration")

    return max(runs, key=lambda run: run.finished_at)


def get_consolidated(
    spark, entity: DataEntity, offset: int, limit: int, integration_id: str | None
) -> ConsolidatedResponse:
    from pyspark.sql import functions as F

    from abassurance.systems.data_integration.pipeline.validation import ID_FIELDS, validate_record_fields
    from abassurance.systems.spark_pipeline.hdfs import HdfsState, read_snapshot
    from abassurance.systems.spark_pipeline.schemas import RAW_SCHEMA

    run = committed_run(integration_id)
    snapshot = run.snapshot

    if not snapshot:
        raise ValueError("Committed run has no snapshot")

    env = Environment.from_file(PROJECT_ROOT / ".env")
    configured_data_kind = env.get("SOURCE_DATA_KIND", "unspecified")
    data_kind: DataKind
    if configured_data_kind == "demo":
        data_kind = "demo"
    elif configured_data_kind == "unspecified":
        data_kind = "unspecified"
    else:
        raise ConfigurationError("SOURCE_DATA_KIND must be demo or unspecified")

    root = f"{env.get('INTEGRATION_HDFS_ROOT', 'hdfs://namenode:9000/abassurance-web').rstrip('/')}/{run.id}"
    storage = HdfsState(spark, root)
    manifest = storage.read_json("CURRENT.json")

    if manifest.get("snapshot") != snapshot:
        raise ValueError("Snapshot identity differs from committed run")

    dataset = "customers" if entity == "clients" else entity
    frame = read_snapshot(spark, root, dataset, manifest=manifest)

    total = frame.count()
    columns = [column for column in frame.columns if column != "_kafka"]

    page = frame.orderBy(ID_FIELDS[dataset]).offset(offset).limit(limit).alias("common")
    raw = spark.read.schema(RAW_SCHEMA).parquet(storage.path("raw")).alias("original")

    joined = (
        page.join(
            raw,
            (F.col("common._kafka.topic") == F.col("original.topic"))
            & (F.col("common._kafka.partition") == F.col("original.partition"))
            & (F.col("common._kafka.offset") == F.col("original.offset")),
            "left",
        )
        .orderBy(F.col(f"common.{ID_FIELDS[dataset]}"))
        .select(
            F.struct(*[F.col(f"common.{column}") for column in columns]).alias("data"),
            F.col("original.value").alias("original"),
        )
    )

    records = []

    for row in joined.collect():
        data = row.data.asDict()
        validate_record_fields(data, dataset, allow_legacy=True)

        envelope = json.loads(row.original)

        if envelope["source_system"] != data["source_system"] or envelope["entity"] != dataset:
            raise ValueError("Archived source does not match the consolidated record")

        original = {
            key: str(value) if type(value) is int and abs(value) > 2**53 - 1 else value
            for key, value in envelope["data"].items()
        }

        records.append(ConsolidatedRecord(data=data, original=original))

    if len(records) != min(limit, max(0, total - offset)):
        raise ValueError("Archived source association is not unique")

    return ConsolidatedResponse(
        entity=entity,
        integration_id=run.id,
        snapshot=snapshot,
        data_kind=data_kind,
        columns=columns,
        records=records,
        offset=offset,
        limit=limit,
        total=total,
    )
