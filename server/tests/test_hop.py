import importlib.util
import json
import os
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from abassurance.systems.data_integration.extraction.config import PROJECT_ROOT


@pytest.fixture
def launcher(monkeypatch, tmp_path):
    path = PROJECT_ROOT / "Infrastructure/hop/run.py"
    spec = importlib.util.spec_from_file_location("hop_launcher", path)

    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    monkeypatch.setenv("ORACLE_APP_PASSWORD", "oracle-test-secret")
    monkeypatch.setenv("SQLSERVER_PASSWORD", "sql-test-secret")
    monkeypatch.setenv("HOP_COUNTS_DIR", str(tmp_path / "counts"))

    return module


@pytest.mark.parametrize("fail", [False, True])
def test_hop_credentials_are_private_not_command_arguments_and_are_removed(launcher, monkeypatch, fail):
    paths = []

    def execute(command, check):
        assert check
        assert "oracle-test-secret" not in str(command)
        assert "sql-test-secret" not in str(command)

        argument = next(arg for arg in command if arg.startswith("--environment-conf-files="))
        path = Path(argument.split("=", 1)[1])
        paths.append(path)

        settings = {item["name"]: item["value"] for item in json.loads(path.read_text())["variables"]}

        assert settings["ORACLE_APP_PASSWORD"] == "oracle-test-secret"
        assert settings["SQLSERVER_PASSWORD"] == "sql-test-secret"

        if os.name != "nt":
            assert path.stat().st_mode & 0o077 == 0

        if fail:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(launcher.subprocess, "run", execute)

    if fail:
        with pytest.raises(subprocess.CalledProcessError):
            launcher.main()
    else:
        launcher.main()

    assert len(paths) == 1
    assert not paths[0].exists()


def test_production_hop_refuses_admin_identity_and_password_fallback(launcher, monkeypatch):
    monkeypatch.setenv("SQLSERVER_REQUIRE_SERVICE_ACCOUNT", "true")
    monkeypatch.setenv("SQLSERVER_USER", "sa")

    with pytest.raises(ValueError, match="dedicated service account"):
        launcher.configuration()

    monkeypatch.setenv("SQLSERVER_USER", "abassurance_reader")
    monkeypatch.delenv("SQLSERVER_PASSWORD")
    monkeypatch.setenv("MSSQL_SA_PASSWORD", "admin-only-secret")

    with pytest.raises(ValueError, match="SQLSERVER_PASSWORD"):
        launcher.configuration()


def test_eight_native_pipelines_read_jdbc_publish_kafka_and_count_sources():
    project = PROJECT_ROOT / "Infrastructure/hop/project"
    pipelines = list((project / "pipelines").glob("*.hpl"))

    assert len(pipelines) == 8

    for path in pipelines:
        root = ET.parse(path).getroot()
        transforms = root.findall("transform")

        types = sorted(transform.findtext("type") or "" for transform in transforms)

        assert types == ["KafkaProducerOutput", "TableInput", "TableInput", "TextFileOutput"]

        producer = next(transform for transform in transforms if transform.findtext("type") == "KafkaProducerOutput")

        assert producer.findtext("keyField") == "message_key"
        assert producer.findtext("messageField") == "message_value"

        topic = producer.findtext("topic")

        assert topic is not None
        assert "${KAFKA_TOPIC_PREFIX}" in topic

        options = {
            option.findtext("property"): option.findtext("value") for option in producer.findall("options/option")
        }

        assert options["acks"] == "all"
        assert options["enable.idempotence"] == "true"

        inputs = [transform.findtext("sql") for transform in transforms if transform.findtext("type") == "TableInput"]

        assert all(sql is not None and sql.strip().startswith("SELECT") for sql in inputs)
        assert any("COUNT(*)" in (sql or "") for sql in inputs)

    workflow = ET.parse(project / "workflows/integrate.hwf").getroot()

    actions = [action for action in workflow.findall("actions/action") if action.findtext("type") == "PIPELINE"]

    assert len(actions) == 8
