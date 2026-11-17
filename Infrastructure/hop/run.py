import json
import os
import re
import subprocess
import tempfile
from pathlib import Path


def configuration():
    defaults = {
        "ORACLE_HOST": "oracle",
        "ORACLE_PORT": "1521",
        "ORACLE_SERVICE": "FREEPDB1",
        "ORACLE_USER": "ABASSURANCE",
        "SQLSERVER_HOST": "sqlserver",
        "SQLSERVER_PORT": "1433",
        "SQLSERVER_DATABASE": "AssurePlus",
        "SQLSERVER_USER": "sa",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
        "KAFKA_TOPIC_PREFIX": "insurance",
        "HOP_COUNTS_DIR": "/tmp/hop-counts",
    }
    values = {key: os.environ.get(key) or default for key, default in defaults.items()}
    for key, fallback in [("ORACLE_APP_PASSWORD", ""), ("SQLSERVER_PASSWORD", "MSSQL_SA_PASSWORD")]:
        value = os.environ.get(key) or os.environ.get(fallback, "")
        if not value:
            raise ValueError(f"Missing {key}")
        values[key] = value
    for name, default in [("SQLSERVER_ENCRYPT", "yes"), ("SQLSERVER_TRUST_CERTIFICATE", "no")]:
        value = os.environ.get(name, default).lower()
        if value not in ("yes", "no", "true", "false", "1", "0"):
            raise ValueError(f"Invalid {name}")
        values["HOP_" + name] = "true" if value in ("yes", "true", "1") else "false"
    prefix = values["KAFKA_TOPIC_PREFIX"]
    if not re.fullmatch(r"[a-zA-Z0-9_-][a-zA-Z0-9._-]*", prefix) or len(prefix) > 225:
        raise ValueError("Invalid Kafka topic prefix")
    return values


def main():
    values = configuration()
    Path(values["HOP_COUNTS_DIR"]).mkdir(parents=True, exist_ok=True)
    home = Path(os.environ.get("HOP_HOME", "/opt/hop"))
    project = Path(os.environ.get("HOP_PROJECT_DIR", str(home / "project")))
    with tempfile.TemporaryDirectory(prefix="abassurance-hop-") as folder:
        settings = Path(folder) / "environment.json"
        settings.write_text(
            json.dumps({"variables": [{"name": key, "value": value} for key, value in values.items()]}),
            encoding="utf-8",
        )
        settings.chmod(0o600)
        command = [
            str(home / "hop-run.sh"),
            "-j",
            "abassurance",
            "-r",
            "local",
            "-l",
            "Basic",
            "-f",
            str(project / "workflows/integrate.hwf"),
            "--environment-conf-files=" + str(settings),
        ]
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
