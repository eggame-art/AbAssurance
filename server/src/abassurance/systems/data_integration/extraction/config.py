from dataclasses import dataclass, field
from pathlib import Path

from abassurance.shared.environment import Environment
from abassurance.shared.errors import ConfigurationError as ConfigurationError

PROJECT_ROOT = Path(__file__).resolve().parents[6]


@dataclass(frozen=True, slots=True)
class OracleConfig:
    user: str
    password: str = field(repr=False)
    dsn: str
    timeout: int = 10


@dataclass(frozen=True, slots=True)
class SQLServerConfig:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)
    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: bool = True
    trust_server_certificate: bool = False
    timeout: int = 10


@dataclass(frozen=True, slots=True)
class Settings:
    oracle: OracleConfig
    sqlserver: SQLServerConfig
    output_dir: Path


def load_oracle_config(env: Environment) -> OracleConfig:
    timeout = env.integer("DB_CONNECT_TIMEOUT", "10", 300)
    oracle_port = env.integer("ORACLE_PORT", "1521", 65535)

    oracle_dsn = env.get("ORACLE_DSN") or (
        f"{env.get('ORACLE_HOST', '127.0.0.1')}:{oracle_port}/{env.get('ORACLE_SERVICE', 'FREEPDB1')}"
    )

    return OracleConfig(
        user=env.get("ORACLE_USER", "ABASSURANCE"),
        password=env.secret("ORACLE_APP_PASSWORD"),
        dsn=oracle_dsn,
        timeout=timeout,
    )


def load_sqlserver_config(env: Environment) -> SQLServerConfig:
    user = env.get("SQLSERVER_USER", "sa")
    dedicated = env.boolean("SQLSERVER_REQUIRE_SERVICE_ACCOUNT", "false")

    if dedicated and user.strip().lower() == "sa":
        raise ConfigurationError("SQL Server requires a dedicated service account")
    password = env.secret("SQLSERVER_PASSWORD", "" if dedicated else env.get("MSSQL_SA_PASSWORD"))

    return SQLServerConfig(
        host=env.get("SQLSERVER_HOST", "127.0.0.1"),
        port=env.integer("SQLSERVER_PORT", "1433", 65535),
        database=env.get("SQLSERVER_DATABASE", "AssurePlus"),
        user=user,
        password=password,
        driver=env.get("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
        encrypt=env.boolean("SQLSERVER_ENCRYPT", "yes"),
        trust_server_certificate=env.boolean("SQLSERVER_TRUST_CERTIFICATE", "no"),
        timeout=env.integer("DB_CONNECT_TIMEOUT", "10", 300),
    )


def load_settings(env_file: Path | None = None) -> Settings:
    path = env_file if env_file is not None else PROJECT_ROOT / ".env"
    env = Environment.from_file(path)
    output_dir = Path(env.get("PIPELINE_OUTPUT_DIR", "data/processed")).expanduser()

    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir

    return Settings(oracle=load_oracle_config(env), sqlserver=load_sqlserver_config(env), output_dir=output_dir)
