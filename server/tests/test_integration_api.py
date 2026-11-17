import json
import subprocess
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from abassurance.systems.integration_api import routes, service, worker
from abassurance.systems.monitoring import service as monitoring
from abassurance.systems.monitoring.app import create_app
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def runtime(tmp_path, monkeypatch):
    job = tmp_path / "hop/project/workflows"
    job.mkdir(parents=True)
    (job / "integrate.hwf").write_text("<workflow/>")
    (job.parent.parent / "hop-run.sh").write_text("test launcher")
    monkeypatch.setattr(service, "state_directory", lambda: tmp_path / "runs")
    monkeypatch.setenv("HOP_PROJECT_DIR", str(job.parent))
    monkeypatch.setenv("HOP_HOME", str(job.parent.parent))
    monkeypatch.setenv("HOP_JAVA_HOME", "test-java-home")
    monkeypatch.setattr(service, "prepare_hop_topics", Mock())
    monkeypatch.setattr(
        service,
        "published_counts",
        lambda config: dict.fromkeys(
            (f"{source}:{entity}" for source, tables in service_source_tables().items() for entity in tables), 2
        ),
    )
    monkeypatch.setenv("PIPELINE_ADMIN_TOKEN", "a" * 64)


@pytest.fixture
def manifest():
    return {
        "snapshot": "b" * 32,
        "format": "parquet",
        "counts": {
            "input": 16,
            "accepted": {"customers": 4, "contracts": 4, "claims": 4, "payments": 4},
            "rejected": 0,
            "duplicates": 0,
            "invalid": 0,
        },
    }


def test_start_is_enforced_on_server_and_concurrency_is_rejected(monkeypatch):
    execute = Mock()
    monkeypatch.setattr(routes, "execute_run", execute)

    with TestClient(create_app()) as client:
        assert client.get("/pipeline/integrations/latest").json() is None
        assert client.post("/pipeline/integrations").status_code == 401
        assert client.post("/pipeline/integrations", headers={"Authorization": "Bearer wrong"}).status_code == 403

        headers = {"Authorization": f"Bearer {'a' * 64}"}
        accepted = client.post("/pipeline/integrations", headers=headers)

        assert accepted.status_code == 202
        assert accepted.json()["status"] == "queued"
        assert client.post("/pipeline/integrations", headers=headers).status_code == 409

        latest = client.get("/pipeline/integrations/latest")

        assert latest.headers["cache-control"] == "no-store"
        assert latest.json() == accepted.json()

    execute.assert_called_once_with(accepted.json()["id"])


def test_missing_admin_configuration_fails_closed(monkeypatch):
    monkeypatch.setenv("PIPELINE_ADMIN_TOKEN", "short")

    with TestClient(create_app()) as client:
        assert client.post("/pipeline/integrations").status_code == 503

    assert service.latest_run() is None


def test_new_api_start_marks_interrupted_job_failed():
    queued = service.create_run()

    with TestClient(create_app()) as client:
        interrupted = client.get("/pipeline/integrations/latest").json()

    assert interrupted["id"] == queued.id
    assert interrupted["status"] == "failed"
    assert interrupted["finished_at"]
    assert interrupted["error"].strip()
    assert interrupted["spark_verified_at"] is None


def test_worker_uses_isolated_topics_and_only_reports_verified_output(monkeypatch, manifest):
    calls = []

    def command(args, env, log):
        assert "PIPELINE_ADMIN_TOKEN" not in env
        assert env["KAFKA_TOPIC_PREFIX"] == f"web-integration-{queued.id}"

        calls.append((args, log.name))
        latest = service.latest_run()

        assert latest is not None

        if log.name == "etl.log":
            write_hop_counts(env, 2)
            log.write_text("Apache Hop completed")
            assert latest.stage == "etl"
        else:
            assert latest.etl_verified_at
            assert latest.stage == "spark"
            assert queued.id in args[-2]

            (log.parent / "result.json").write_text(json.dumps(manifest))

    monkeypatch.setattr(service, "run_command", command)

    queued = service.create_run()
    service.execute_run(queued.id)
    latest = service.latest_run()

    assert latest is not None
    assert latest.status == "succeeded"
    assert latest.stage == "completed"
    assert latest.counts is not None
    assert latest.counts.input == 16
    assert latest.spark_verified_at and latest.finished_at
    assert [log for _, log in calls] == ["etl.log", "spark.log"]
    assert latest.snapshot == "b" * 32
    assert (service.state_directory() / f"{queued.id}.json").is_file()


@pytest.mark.parametrize("stage", ["etl", "spark"])
def test_failed_stage_does_not_report_success_or_leak_diagnostics(monkeypatch, stage):
    def command(args, env, log):
        if log.name == f"{stage}.log":
            raise RuntimeError("private-password secret-host")

        write_hop_counts(env, 2)
        log.write_text("Apache Hop completed")

    monkeypatch.setattr(service, "run_command", command)

    queued = service.create_run()
    service.execute_run(queued.id)
    failed = service.latest_run()

    assert failed is not None
    assert failed.status == "failed"
    assert failed.stage == stage
    assert failed.finished_at
    assert failed.snapshot is None
    assert failed.spark_verified_at is None
    assert (failed.etl_verified_at is not None) == (stage == "spark")
    assert "private-password" not in failed.model_dump_json()
    assert "secret-host" not in failed.model_dump_json()


def test_missing_acknowledgment_stops_before_spark(monkeypatch):
    command = Mock(side_effect=lambda args, env, log: log.write_text("process exited but no success acknowledgment"))
    monkeypatch.setattr(service, "run_command", command)

    queued = service.create_run()
    service.execute_run(queued.id)
    failed = service.latest_run()

    assert failed is not None
    assert failed.status == "failed"
    assert failed.stage == "etl"
    assert command.call_count == 1


def test_mismatched_source_and_snapshot_counts_cannot_succeed(monkeypatch, manifest):
    def command(args, env, log):
        if log.name == "etl.log":
            write_hop_counts(env, 999)
            log.write_text("Apache Hop completed")
            monkeypatch.setattr(
                service,
                "published_counts",
                lambda config: dict.fromkeys(
                    (f"{source}:{entity}" for source, tables in service_source_tables().items() for entity in tables),
                    999,
                ),
            )
        else:
            (log.parent / "result.json").write_text(json.dumps(manifest))

    monkeypatch.setattr(service, "run_command", command)

    run = service.create_run()
    service.execute_run(run.id)
    failed = service.latest_run()

    assert failed is not None
    assert failed.status == "failed"
    assert failed.stage == "verification"
    assert failed.spark_verified_at is None


def test_timeout_stops_subprocess_and_never_enables_shell(monkeypatch, tmp_path):
    process = Mock()
    process.wait.side_effect = [subprocess.TimeoutExpired("command", 600), None]

    popen = Mock(return_value=process)
    monkeypatch.setattr(service.subprocess, "Popen", popen)
    monkeypatch.setattr(service.os, "killpg", Mock(), raising=False)

    with pytest.raises(subprocess.TimeoutExpired):
        service.run_command(["command"], {}, tmp_path / "log")

    assert "shell" not in popen.call_args.kwargs
    assert process.wait.call_count == 2


def test_snapshot_is_read_again_before_success(monkeypatch, tmp_path, manifest):
    spark = Mock()

    monkeypatch.setattr(worker, "create_session", lambda: spark)
    monkeypatch.setattr(worker, "load_kafka_config", lambda: object())
    monkeypatch.setattr(worker, "run_available", lambda *args: manifest)

    reader = Mock(
        side_effect=lambda spark, state, entity, **kwargs: SimpleNamespace(
            count=lambda: 0 if entity == "rejected" else 4,
            drop=lambda _: SimpleNamespace(schema=worker.entity_schema(entity)),
        )
    )

    monkeypatch.setattr(worker, "read_snapshot", reader)

    target = tmp_path / "result.json"
    monkeypatch.setattr("sys.argv", ["worker", "hdfs://namenode:9000/test", str(target)])

    worker.main()

    assert reader.call_count == 5
    assert json.loads(target.read_text()) == manifest
    spark.stop.assert_called_once()


def test_batch_status_requires_real_completed_job_and_healthy_runtime(monkeypatch):
    now = datetime.now(UTC)
    monkeypatch.setattr(monitoring, "latest_run", lambda: None)

    env = service.Environment.from_file(service.PROJECT_ROOT / ".env")

    with pytest.raises(monitoring.CheckNotConfigured):
        monitoring.check_etl(env)

    monkeypatch.setattr(monitoring, "latest_run", lambda: SimpleNamespace(etl_verified_at=now, etl_engine="apache-hop"))

    java = Mock()
    monkeypatch.setattr(monitoring.subprocess, "run", java)

    message = monitoring.check_etl(env)

    assert now.strftime("%H:%M:%S") in message
    assert "UTC" in message

    java.assert_called_once()
    java.side_effect = RuntimeError("Java failed")

    with pytest.raises(RuntimeError):
        monitoring.check_etl(env)


@pytest.fixture(autouse=True)
def isolate_route_contracts_from_authentication(monkeypatch):
    async def passthrough(request, call_next):
        return await call_next(request)

    monkeypatch.setattr("abassurance.systems.monitoring.app.protect", passthrough)
    monkeypatch.setattr("abassurance.systems.prediction_api.app.protect", passthrough)


def service_source_tables():
    from abassurance.systems.messaging.config import SOURCE_TABLES

    return SOURCE_TABLES


def write_hop_counts(env, count):
    from pathlib import Path

    folder = Path(env["HOP_COUNTS_DIR"])
    folder.mkdir(parents=True, exist_ok=True)
    for source, tables in service_source_tables().items():
        for entity in tables:
            (folder / f"{source}_{entity}.txt").write_text(str(count))


def test_hop_missing_kafka_publications_never_reach_spark(monkeypatch):
    calls = []

    def command(args, env, log):
        calls.append(log.name)
        write_hop_counts(env, 2)

    monkeypatch.setattr(service, "run_command", command)
    monkeypatch.setattr(
        service,
        "published_counts",
        lambda config: dict.fromkeys(
            (f"{source}:{entity}" for source, tables in service_source_tables().items() for entity in tables), 0
        ),
    )
    queued = service.create_run()
    service.execute_run(queued.id)
    run = service.latest_run()
    assert run is not None
    assert run.status == "failed" and run.stage == "etl"
    assert run.etl_verified_at is None and run.snapshot is None
    assert calls == ["etl.log"]


def test_a_legacy_etl_success_does_not_validate_apache_hop(monkeypatch):
    now = datetime.now(UTC)
    monkeypatch.setattr(monitoring, "latest_run", lambda: SimpleNamespace(etl_verified_at=now, etl_engine=None))
    with pytest.raises(monitoring.CheckNotConfigured, match="Apache Hop"):
        monitoring.check_etl(service.Environment.from_file(service.PROJECT_ROOT / ".env"))
