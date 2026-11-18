import json
import os
import re
import signal
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from uuid import uuid4

from abassurance.shared.environment import Environment
from abassurance.shared.storage import state_lock, write_json_atomic
from abassurance.systems.data_integration.extraction.config import PROJECT_ROOT
from abassurance.systems.integration_api.schemas import IntegrationCounts, IntegrationRun

STAGE_TIMEOUT = 600
START_LOCK = Lock()

RUNTIME_VARIABLES = {
    "PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "TZ",
    "TMPDIR",
    "TMP",
    "TEMP",
    "SystemRoot",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
    "USERPROFILE",
    "APPDATA",
    "LOCALAPPDATA",
    "JAVA_HOME",
    "PYTHONPATH",
    "PYSPARK_PYTHON",
    "PYSPARK_DRIVER_PYTHON",
    "SPARK_LOCAL_IP",
    "SPARK_LOCAL_HOSTNAME",
    "SPARK_SUBMIT_OPTS",
    "HADOOP_HOME",
    "HADOOP_CONF_DIR",
    "HADOOP_USER_NAME",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_GROUP_ID",
    "KAFKA_TIMEOUT",
    "INTEGRATION_HDFS_ROOT",
}
ETL_VARIABLES = {
    "HOP_HOME",
    "HOP_PROJECT_DIR",
    "HOP_CONFIG_FOLDER",
    "HOP_JAVA_HOME",
    "HOP_OPTIONS",
    "HOP_COUNTS_DIR",
    "ORACLE_HOST",
    "ORACLE_PORT",
    "ORACLE_SERVICE",
    "ORACLE_USER",
    "ORACLE_APP_PASSWORD",
    "ORACLE_APP_PASSWORD_FILE",
    "SQLSERVER_HOST",
    "SQLSERVER_PORT",
    "SQLSERVER_DATABASE",
    "SQLSERVER_USER",
    "SQLSERVER_PASSWORD",
    "SQLSERVER_PASSWORD_FILE",
    "SQLSERVER_REQUIRE_SERVICE_ACCOUNT",
    "SQLSERVER_ENCRYPT",
    "SQLSERVER_TRUST_CERTIFICATE",
}

STAGE_LABELS = {
    "pending": "Preparation",
    "etl": "Apache Hop into Kafka",
    "spark": "Spark into HDFS",
    "verification": "Snapshot verification",
    "completed": "Finalization",
}


class IntegrationBusy(Exception):
    pass


def state_directory() -> Path:
    env = Environment.from_file(PROJECT_ROOT / ".env")
    return Path(env.get("INTEGRATION_STATE_DIR", str(PROJECT_ROOT / "data/processed/integrations")))


def latest_run() -> IntegrationRun | None:
    path = state_directory() / "latest.json"
    return IntegrationRun.model_validate_json(path.read_text("utf-8")) if path.exists() else None


def save_run(run: IntegrationRun) -> None:
    root = state_directory()
    root.mkdir(parents=True, exist_ok=True)

    data = run.model_dump(mode="json")
    write_json_atomic(root / f"{run.id}.json", data)
    write_json_atomic(root / "latest.json", data)


def create_run() -> IntegrationRun:
    from abassurance.systems.spark_pipeline.hdfs import validate_hdfs_root

    with START_LOCK, state_lock(state_directory()):
        previous = latest_run()

        if previous and previous.status in {"queued", "running"}:
            raise IntegrationBusy

        env = Environment.from_file(PROJECT_ROOT / ".env")
        job = Path(env.required("HOP_PROJECT_DIR"))

        if not (job / "workflows/integrate.hwf").is_file():
            raise FileNotFoundError("ETL job is not installed")

        validate_hdfs_root(env.get("INTEGRATION_HDFS_ROOT", "hdfs://namenode:9000/abassurance-web"))

        run = IntegrationRun(id=uuid4().hex, status="queued", created_at=datetime.now(UTC), etl_engine="apache-hop")
        save_run(run)

        return run


def recover_interrupted() -> None:
    with state_lock(state_directory()):
        run = latest_run()

        if not run or run.status not in {"queued", "running"}:
            return

        run.status = "failed"
        run.finished_at = datetime.now(UTC)
        run.error = "Processing was interrupted by a restart. A new integration is required."

        save_run(run)


def run_command(command: list[str], env: dict[str, str], log_path: Path) -> None:
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            command, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=os.name != "nt"
        )

        try:
            code = process.wait(timeout=STAGE_TIMEOUT)
        except BaseException:
            if os.name == "nt":
                process.kill()
            else:
                os.killpg(process.pid, signal.SIGKILL)

            process.wait()
            raise

    if code != 0:
        raise RuntimeError("Pipeline command failed")


def load_environment(run_id: str) -> dict[str, str]:
    env = {
        name: value
        for name, value in Environment.from_file(PROJECT_ROOT / ".env").values.items()
        if value is not None and name in RUNTIME_VARIABLES | ETL_VARIABLES
    }

    env["KAFKA_TOPIC_PREFIX"] = f"web-integration-{run_id}"

    return env


def prepare_hop_topics(config) -> None:
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.cimpl import NewTopic

    admin = AdminClient({"bootstrap.servers": config.bootstrap_servers})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in config.topics()]
    )
    for future in futures.values():
        future.result(timeout=15)


def published_counts(config) -> dict[str, int]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": config.bootstrap_servers, "group.id": "hop-verification-" + uuid4().hex})
    try:
        metadata = consumer.list_topics(timeout=10)
        counts = {}
        from abassurance.systems.messaging.config import SOURCE_TABLES

        for source, tables in SOURCE_TABLES.items():
            for entity in tables:
                topic = config.topic(source, entity)
                info = metadata.topics[topic]
                if info.error:
                    raise RuntimeError("Kafka topic metadata unavailable")
                total = 0
                for partition in info.partitions:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(topic, partition), timeout=10, cached=False
                    )
                    total += high - low
                counts[f"{source}:{entity}"] = total
        return counts
    finally:
        consumer.close()


def execute_etl(run: IntegrationRun, env: dict[str, str], folder: Path) -> dict:
    from abassurance.systems.messaging.config import SOURCE_TABLES, KafkaConfig

    config = KafkaConfig(bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"], topic_prefix=env["KAFKA_TOPIC_PREFIX"])
    run.status = "running"
    run.stage = "etl"
    run.started_at = datetime.now(UTC)
    save_run(run)
    prepare_hop_topics(config)
    counts_dir = folder / "hop-counts"
    env["HOP_COUNTS_DIR"] = str(counts_dir)
    home = Path(env["HOP_HOME"])
    run_command([sys.executable, str(home / "run.py")], env, folder / "etl.log")
    expected = {}
    for source, tables in SOURCE_TABLES.items():
        for entity in tables:
            text = (counts_dir / f"{source}_{entity}.txt").read_text("utf-8").strip()
            if not re.fullmatch(r"[0-9]+", text):
                raise ValueError("Hop source count is invalid")
            expected[f"{source}:{entity}"] = int(text)
    if published_counts(config) != expected:
        raise ValueError("Hop source counts and Kafka publication counts differ")
    run.etl_verified_at = datetime.now(UTC)
    run.stage = "spark"
    save_run(run)
    return expected


def execute_spark(run: IntegrationRun, env: dict[str, str], folder: Path) -> dict:
    root = env.get("INTEGRATION_HDFS_ROOT", "hdfs://namenode:9000/abassurance-web").rstrip("/")
    state = f"{root}/{run.id}"
    result = folder / "result.json"

    command = [sys.executable, "-m", "abassurance.systems.integration_api.worker", state, str(result)]

    worker_env = {
        name: value for name, value in env.items() if name in RUNTIME_VARIABLES or name == "KAFKA_TOPIC_PREFIX"
    }
    run_command(command, worker_env, folder / "spark.log")

    run.stage = "verification"
    save_run(run)

    return json.loads(result.read_text("utf-8"))


def verify_snapshot(run: IntegrationRun, manifest: dict, published: dict) -> None:
    counts = IntegrationCounts.model_validate(manifest["counts"])

    accepted = sum(counts.accepted.model_dump().values())

    if counts.input != sum(published.values()) or counts.input != accepted + counts.rejected:
        raise ValueError("Source and persisted counts differ")

    if counts.rejected != counts.duplicates + counts.invalid:
        raise ValueError("Rejection accounting differs")

    if not re.fullmatch(r"[0-9a-f]{32}", manifest["snapshot"]):
        raise ValueError("Snapshot identity invalid")

    run.snapshot = manifest["snapshot"]
    run.counts = counts
    run.spark_verified_at = datetime.now(UTC)
    run.status = "succeeded"
    run.stage = "completed"


def execute_run(run_id: str) -> None:
    root = state_directory()
    run = IntegrationRun.model_validate_json((root / f"{run_id}.json").read_text("utf-8"))

    folder = root / run.id
    folder.mkdir(exist_ok=True)

    try:
        env = load_environment(run.id)
        published = execute_etl(run, env, folder)
        manifest = execute_spark(run, env, folder)

        verify_snapshot(run, manifest, published)

    except Exception:
        run.status = "failed"
        stage_name = STAGE_LABELS[run.stage]
        run.error = f"Failure of the step {stage_name}. Detailed diagnostics are logged in the server logs."

    finally:
        run.finished_at = datetime.now(UTC)
        save_run(run)
