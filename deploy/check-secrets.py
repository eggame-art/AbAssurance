import importlib.util
import os
from pathlib import Path

from abassurance.shared.environment import Environment
from abassurance.systems.data_integration.extraction.config import load_oracle_config, load_sqlserver_config
from abassurance.systems.integration_api.service import load_environment

ADMIN_SECRETS = ("ORACLE_PASSWORD", "MSSQL_SA_PASSWORD", "SQLCMDPASSWORD", "ORACLE_APP_PASSWORD", "SQLSERVER_PASSWORD")

for name in ADMIN_SECRETS:
    assert name not in os.environ, f"Unexpected secret in environment: {name}"

assert not Path("/app/.env").exists(), "Full .env must not be available to backend"

env = Environment(os.environ)

oracle = load_oracle_config(env)
sqlserver = load_sqlserver_config(env)

assert oracle.password
assert sqlserver.password
assert sqlserver.user == "abassurance_reader"

spec = importlib.util.spec_from_file_location("hop_launcher", "/opt/hop/run.py")

assert spec is not None
assert spec.loader is not None

hop = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hop)

values = hop.configuration()

assert values["ORACLE_APP_PASSWORD"] == oracle.password
assert values["SQLSERVER_PASSWORD"] == sqlserver.password

child = load_environment("secret-check")

assert "SQLSERVER_PASSWORD_FILE" in child
assert "SQLSERVER_PASSWORD" not in child

print("Backend and Hop can read service secrets; administrator credentials are absent from backend environment.")
