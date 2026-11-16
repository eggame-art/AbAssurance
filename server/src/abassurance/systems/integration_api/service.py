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

STAGE_LABELS = {
    "pending": "Preparation",
    "etl": "Talaxie into Kafka",
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
        job = Path(env.required("TALAXIE_JOB_DIR"))

        if not (job / "ExtractPublish/extractpublish_0_1.jar").is_file():
            raise FileNotFoundError("ETL job is not installed")

        validate_hdfs_root(env.get("INTEGRATION_HDFS_ROOT", "hdfs://namenode:9000/abassurance-web"))

        run = IntegrationRun(id=uuid4().hex, status="queued", created_at=datetime.now(UTC))
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
        name: value for name, value in Environment.from_file(PROJECT_ROOT / ".env").values.items() if value is not None
    }

    env["KAFKA_TOPIC_PREFIX"] = f"web-integration-{run_id}"
    env.pop("PIPELINE_ADMIN_TOKEN", None)

    return env


def execute_etl(run: IntegrationRun, env: dict[str, str], folder: Path) -> dict:
    job = Path(env["TALAXIE_JOB_DIR"])

    run.status = "running"
    run.stage = "etl"
    run.started_at = datetime.now(UTC)
    save_run(run)

    command = [
        env["TALAXIE_JAVA"],
        "-Duser.timezone=UTC",
        "-Dfile.encoding=UTF-8",
        "-cp",
        f"{job}/ExtractPublish:{job}/lib/*:{job}/ExtractPublish/extractpublish_0_1.jar",
        "abassurance.extractpublish_0_1.ExtractPublish",
        "--context=Default",
    ]

    run_command(command, env, folder / "etl.log")

    output = (folder / "etl.log").read_text("utf-8", errors="replace")
    match = re.search(r"^TALAXIE status=succeeded published=(\{.*\})$", output, re.MULTILINE)

    if not match:
        raise ValueError("ETL acknowledgment missing")

    published = json.loads(match.group(1))

    if not isinstance(published, dict) or not all(type(value) is int and value >= 0 for value in published.values()):
        raise ValueError("ETL acknowledgment invalid")

    run.etl_verified_at = datetime.now(UTC)
    run.stage = "spark"
    save_run(run)

    return published


def execute_spark(run: IntegrationRun, env: dict[str, str], folder: Path) -> dict:
    root = env.get("INTEGRATION_HDFS_ROOT", "hdfs://namenode:9000/abassurance-web").rstrip("/")
    state = f"{root}/{run.id}"
    result = folder / "result.json"

    command = [sys.executable, "-m", "abassurance.systems.integration_api.worker", state, str(result)]

    run_command(command, env, folder / "spark.log")

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
