import os
from pathlib import Path

import pytest

from extraction.config import ConfigurationError, load_settings


@pytest.fixture
def env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for name in os.environ:
        if (name.startswith(("ORACLE_", "SQLSERVER_", "MSSQL_", "DB_", "PIPELINE_"))):
            monkeypatch.delenv(name)
    path = tmp_path / ".env"
    path.write_text("ORACLE_APP_PASSWORD=oracle-test-secret\nMSSQL_SA_PASSWORD=sql-test-secret\n", encoding="utf-8")
    return path


def test_environment_overrides_file_and_secrets_are_not_repr(env_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSERVER_PORT", "15433")
    monkeypatch.setenv("ORACLE_PORT", "15521")
    monkeypatch.setenv("SQLSERVER_DATABASE", "Alternative")
    monkeypatch.setenv("SQLSERVER_PASSWORD", "dedicated-account-secret")
    monkeypatch.setenv("PIPELINE_OUTPUT_DIR", str(env_file.parent / "output"))
    settings = load_settings(env_file)
    assert settings.oracle.dsn == "127.0.0.1:15521/FREEPDB1"
    assert settings.sqlserver.port == 15433
    assert settings.sqlserver.database == "Alternative"
    assert settings.sqlserver.password == "dedicated-account-secret"
    assert settings.output_dir == env_file.parent / "output"
    assert "secret" not in repr(settings)
    assert "ORACLE_APP_PASSWORD" not in os.environ


@pytest.mark.parametrize(
    "name,value",
    [
        ("SQLSERVER_PORT", "0"),
        ("ORACLE_PORT", "65536"),
        ("DB_CONNECT_TIMEOUT", "invalid"),
        ("SQLSERVER_ENCRYPT", "maybe"),
    ],
)
def test_invalid_configuration(env_file: Path, monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigurationError, match=name):
        load_settings(env_file)


def test_missing_password_is_explicit(env_file: Path) -> None:
    env_file.write_text("", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="ORACLE_APP_PASSWORD"):
        load_settings(env_file)


def test_dsn_and_tls_configuration(env_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_DSN", "database.example.test:1522/CUSTOM")
    monkeypatch.setenv("SQLSERVER_TRUST_CERTIFICATE", "true")
    settings = load_settings(env_file)
    assert settings.oracle.dsn == "database.example.test:1522/CUSTOM"
    assert settings.sqlserver.encrypt is True
    assert settings.sqlserver.trust_server_certificate is True
