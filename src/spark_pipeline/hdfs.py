import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import uuid4

from messaging.config import KafkaConfig
from pipeline.validation import ID_FIELDS
from shared.errors import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def validate_hdfs_root(root: str) -> str:
    uri = urlsplit(root)

    if (
        uri.scheme != "hdfs"
        or not uri.hostname
        or uri.username
        or uri.password
        or uri.query
        or uri.fragment
        or uri.path in ("", "/")
        or any(part in ("", ".", "..") for part in uri.path.strip("/").split("/"))
        or "\\" in root
    ):
        raise ConfigurationError("Use an absolute HDFS state URI, such as hdfs://namenode:9000/abassurance")

    try:
        port = uri.port
    except ValueError as exc:
        raise ConfigurationError("Invalid HDFS port") from exc

    if port is not None and port < 1:
        raise ConfigurationError("Invalid HDFS port")

    return root.rstrip("/")


class HdfsState:
    def __init__(self, spark: "SparkSession", root: str) -> None:
        self.root = validate_hdfs_root(root)
        # Py4J resolves Java classes and methods dynamically at runtime.
        self.jvm: Any = spark.sparkContext._jvm
        self.gateway: Any = spark.sparkContext._gateway
        java_context: Any = spark.sparkContext._jsc

        if self.jvm is None or self.gateway is None or java_context is None:
            raise ConfigurationError("HDFS state requires an active Spark JVM context")

        self.configuration = java_context.hadoopConfiguration()
        self.fs = self.jvm.org.apache.hadoop.fs.Path(self.root).getFileSystem(self.configuration)
        self.context = self.jvm.org.apache.hadoop.fs.FileContext.getFileContext(
            self.jvm.java.net.URI(self.root), self.configuration
        )

    def path(self, relative: str) -> str:
        return f"{self.root}/{relative}"

    def java_path(self, relative: str) -> Any:
        return self.jvm.org.apache.hadoop.fs.Path(self.path(relative))

    def exists(self, relative: str) -> bool:
        return bool(self.fs.exists(self.java_path(relative)))

    def mkdir(self, relative: str = "") -> None:
        if not self.fs.mkdirs(self.java_path(relative)):
            raise OSError(f"Cannot create HDFS directory: {self.path(relative)}")

    def read_json(self, relative: str) -> dict[str, Any]:
        stream = self.fs.open(self.java_path(relative))

        try:
            content = self.jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")

            return json.loads(content)

        finally:
            stream.close()

    def write_json_atomic(self, relative: str, value: dict[str, Any]) -> None:
        temporary = self.java_path(f"{relative}.{uuid4().hex}.tmp")

        try:
            stream = self.fs.create(temporary, False)

            try:
                content = json.dumps(value, ensure_ascii=False, indent=2) + "\n"
                stream.write(bytearray(content, "utf-8"))
                stream.hsync()

            finally:
                stream.close()

            rename = self.jvm.org.apache.hadoop.fs.Options.Rename
            options = self.gateway.new_array(rename, 1)
            options[0] = rename.OVERWRITE

            self.context.rename(temporary, self.java_path(relative), options)

        finally:
            self.fs.delete(temporary, False)

    @contextmanager
    def lock(self) -> Iterator[None]:
        self.mkdir()
        path = self.java_path(".lock")

        try:
            stream = self.fs.create(path, False)
        except Exception as exc:
            if self.exists(".lock"):
                raise ConfigurationError("HDFS state already in use; check the writer before removing .lock") from exc

            raise

        try:
            stream.close()

            yield

        finally:
            self.fs.delete(path, False)

    def bind(self, config: KafkaConfig) -> None:
        binding = {"version": 1, "bootstrap_servers": config.bootstrap_servers, "topics": config.topics()}

        if self.exists("source.json"):
            if self.read_json("source.json") != binding:
                raise ConfigurationError("Spark source changed: use a new state directory")

            if self.exists("raw") != self.exists("checkpoint"):
                raise ConfigurationError("Keep Spark raw and checkpoint together; use a new state directory to replay")

        else:
            if self.exists("raw") or self.exists("checkpoint"):
                raise ConfigurationError("Unbound Spark raw/checkpoint directory; use a new state directory")

            self.write_json_atomic("source.json", binding)


def materialize_hdfs(spark: "SparkSession", state: HdfsState, prefix: str) -> dict[str, Any]:
    from spark_pipeline.schemas import RAW_SCHEMA
    from spark_pipeline.transform import Snapshot

    raw = spark.read.schema(RAW_SCHEMA).parquet(state.path("raw"))
    snapshot = Snapshot(raw, prefix)

    try:
        counts = snapshot.counts()
        run_id = uuid4().hex
        destination = f"snapshots/{run_id}"

        for entity, frame in snapshot.datasets.items():
            frame.write.mode("errorifexists").parquet(state.path(f"{destination}/unified/{entity}"))

        snapshot.rejected.write.mode("errorifexists").parquet(state.path(f"{destination}/rejected"))

        manifest = {"snapshot": run_id, "format": "parquet", "counts": counts}

        state.write_json_atomic(f"{destination}/report.json", manifest)
        state.write_json_atomic("CURRENT.json", manifest)

        return manifest

    finally:
        snapshot.close()


def read_snapshot(
    spark: "SparkSession", root: str, entity: str, *, manifest: dict[str, Any] | None = None
) -> "DataFrame":
    if entity not in (*ID_FIELDS, "rejected"):
        raise ConfigurationError(f"Unknown snapshot entity: {entity}")

    state = HdfsState(spark, root)
    manifest = state.read_json("CURRENT.json") if (manifest is None) else manifest
    run_id = manifest["snapshot"]

    if manifest.get("format") != "parquet" or not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise ConfigurationError("Invalid HDFS snapshot manifest")

    folder = "rejected" if (entity == "rejected") else f"unified/{entity}"

    return spark.read.parquet(state.path(f"snapshots/{run_id}/{folder}"))
