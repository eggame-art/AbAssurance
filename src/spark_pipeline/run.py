import argparse
import json
import logging
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from messaging.config import KafkaConfig, load_kafka_config
from shared.errors import ConfigurationError

if (TYPE_CHECKING):
    from pyspark.sql import DataFrame, SparkSession

LOGGER = logging.getLogger(__name__)
CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"


def create_session(*, kafka: bool = True, master: str = "local[2]") -> "SparkSession":
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = (
        SparkSession.Builder()
        .appName("AbAssurance-Spark")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.maxPlanStringLength", "8192")
        .config("spark.sql.ui.retainedExecutions", "20")
    )

    if kafka:
        builder = builder.config("spark.jars.packages", CONNECTOR)

    return builder.getOrCreate()


@contextmanager
def state_lock(state: Path) -> Iterator[None]:
    state.mkdir(parents=True, exist_ok=True)

    with (state / ".lock").open("a+b") as handle:
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()

        handle.seek(0)

        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)

        except OSError as exc:
            raise ConfigurationError("Spark state directory already in use") from exc

        try:
            yield

        finally:
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)


def write_json_atomic(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")

    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    temporary.replace(path)


def bind_state(state: Path, config: KafkaConfig) -> None:
    binding = {"version": 1, "bootstrap_servers": config.bootstrap_servers, "topics": config.topics()}

    path = state / "source.json"

    if path.exists():
        if json.loads(path.read_text("utf-8")) != binding:
            raise ConfigurationError("Spark source changed: use a new state directory")

        if (state / "raw").exists() != (state / "checkpoint").exists():
            raise ConfigurationError("Keep Spark raw and checkpoint together; use a new state directory to replay")

    else:
        if (state / "raw").exists() or (state / "checkpoint").exists():
            raise ConfigurationError("Unbound Spark raw/checkpoint directory; use a new state directory")

        write_json_atomic(path, binding)


def kafka_stream(spark: "SparkSession", config: KafkaConfig, max_offsets: int) -> "DataFrame":
    if max_offsets < 1:
        raise ConfigurationError("max-offsets must be positive")

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", ",".join(config.topics()))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", str(max_offsets))
        .load()
        .selectExpr(
            "CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value", "topic", "partition", "offset", "timestamp"
        )
    )


def ingest_available(stream: "DataFrame", state: Path | str) -> None:
    query = (
        stream.writeStream.format("parquet")
        .option("path", f"{state}/raw")
        .option("checkpointLocation", f"{state}/checkpoint")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    try:
        query.awaitTermination()

    finally:
        if query.isActive:
            query.stop()


def materialize(spark: "SparkSession", state: Path, prefix: str) -> dict[str, Any]:
    from spark_pipeline.schemas import RAW_SCHEMA
    from spark_pipeline.transform import Snapshot

    raw = spark.read.schema(RAW_SCHEMA).parquet(str(state / "raw"))
    snapshot = Snapshot(raw, prefix)

    try:
        counts = snapshot.counts()
        run_id = uuid4().hex

        destination = state / "snapshots" / run_id
        destination.mkdir(parents=True)

        for entity, frame in {**snapshot.datasets, "rejected": snapshot.rejected}.items():
            frame.write.mode("errorifexists").option("ignoreNullFields", "false").json(str(destination / entity))

        manifest = {"snapshot": run_id, "counts": counts}

        write_json_atomic(destination / "report.json", manifest)
        write_json_atomic(state / "CURRENT.json", manifest)

        return manifest

    finally:
        snapshot.close()


def run_available(
    spark: "SparkSession", config: KafkaConfig, state: Path | str, *, max_offsets: int = 10000
) -> dict[str, Any]:
    if (isinstance(state, str) and state.startswith("hdfs:")):
        from spark_pipeline.hdfs import HdfsState, materialize_hdfs

        storage = HdfsState(spark, state)

        with storage.lock():
            storage.bind(config)

            ingest_available(kafka_stream(spark, config, max_offsets), storage.root)

            return materialize_hdfs(spark, storage, config.topic_prefix)

    if (isinstance(state, str) and "://" in state):
        raise ConfigurationError("Unsupported state URI; use a local directory or hdfs:// URI")

    state = Path(state).resolve()

    with state_lock(state):
        bind_state(state, config)

        ingest_available(kafka_stream(spark, config, max_offsets), state)

        return materialize(spark, state, config.topic_prefix)


def main() -> int:
    parser = argparse.ArgumentParser()

    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--state-dir", default=os.environ.get("SPARK_STATE_DIR", "data/spark"))
    parser.add_argument("--max-offsets", type=int, default=10000)
    parser.add_argument("--master", default="local[2]")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    spark = None

    try:
        if args.max_offsets < 1:
            raise ConfigurationError("max-offsets must be positive")

        config = load_kafka_config(args.env_file)

        spark = create_session(master=args.master)
        spark.sparkContext.setLogLevel("WARN")

        manifest = run_available(spark, config, args.state_dir, max_offsets=args.max_offsets)

        print(json.dumps(manifest, ensure_ascii=False, indent=2))

        return 0

    except Exception:
        LOGGER.exception("Spark processing failed")

        return 1

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
