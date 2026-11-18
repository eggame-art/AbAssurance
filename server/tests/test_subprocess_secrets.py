from abassurance.shared.environment import Environment
from abassurance.systems.integration_api import service


def test_integration_only_forwards_allowed_variables(monkeypatch):
    values = {
        "PATH": "/usr/bin",
        "JAVA_HOME": "/opt/java",
        "HOP_HOME": "/opt/hop",
        "ORACLE_APP_PASSWORD_FILE": "/run/secrets/oracle_app_password",
        "SQLSERVER_PASSWORD_FILE": "/run/secrets/sqlserver_app_password",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
        "ORACLE_PASSWORD": "oracle-admin-secret",
        "MSSQL_SA_PASSWORD": "sql-admin-secret",
        "SQLCMDPASSWORD": "sql-admin-secret",
        "PIPELINE_ADMIN_TOKEN": "pipeline-secret",
        "AWS_SECRET_ACCESS_KEY": "unrelated-secret",
        "LD_PRELOAD": "untrusted-library",
    }

    monkeypatch.setattr(Environment, "from_file", lambda _: Environment(values))
    result = service.load_environment("test-run")

    assert result == {
        key: value
        for key, value in values.items()
        if key
        in {
            "PATH",
            "JAVA_HOME",
            "HOP_HOME",
            "ORACLE_APP_PASSWORD_FILE",
            "SQLSERVER_PASSWORD_FILE",
            "KAFKA_BOOTSTRAP_SERVERS",
        }
    } | {"KAFKA_TOPIC_PREFIX": "web-integration-test-run"}
