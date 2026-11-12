from dataclasses import dataclass, field
from pathlib import Path

from shared.environment import Environment
from shared.errors import ConfigurationError as ConfigurationError

PROJECT_ROOT = Path(__file__).resolve().parents[2]


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


def load_settings(env_file: Path | None = None) -> Settings:
    path = env_file if env_file is not None else PROJECT_ROOT / ".env"
    env = Environment.from_file(path)

    timeout = env.integer("DB_CONNECT_TIMEOUT", "10", 300)
    oracle_port = env.integer("ORACLE_PORT", "1521", 65535)

    oracle_dsn = env.get("ORACLE_DSN") or (
        f"{env.get('ORACLE_HOST', '127.0.0.1')}:{oracle_port}/{env.get('ORACLE_SERVICE', 'FREEPDB1')}"
    )

    output_dir = Path(env.get("PIPELINE_OUTPUT_DIR", "data/processed")).expanduser()

    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir

    return Settings(
        oracle=OracleConfig(
            user=env.get("ORACLE_USER", "ABASSURANCE"),
            password=env.required("ORACLE_APP_PASSWORD"),
            dsn=oracle_dsn,
            timeout=timeout,
        ),
        sqlserver=SQLServerConfig(
            host=env.get("SQLSERVER_HOST", "127.0.0.1"),
            port=env.integer("SQLSERVER_PORT", "1433", 65535),
            database=env.get("SQLSERVER_DATABASE", "AssurePlus"),
            user=env.get("SQLSERVER_USER", "sa"),
            password=env.required("SQLSERVER_PASSWORD", env.get("MSSQL_SA_PASSWORD")),
            driver=env.get("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
            encrypt=env.boolean("SQLSERVER_ENCRYPT", "yes"),
            trust_server_certificate=env.boolean("SQLSERVER_TRUST_CERTIFICATE", "no"),
            timeout=timeout,
        ),
        output_dir=output_dir,
    )
