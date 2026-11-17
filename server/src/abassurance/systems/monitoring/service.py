import asyncio
import json
import os
import shutil
import subprocess
from collections.abc import Callable
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen

from abassurance.shared.environment import Environment
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_integration.extraction.config import PROJECT_ROOT, OracleConfig, load_sqlserver_config
from abassurance.systems.integration_api.service import latest_run
from abassurance.systems.monitoring.schemas import ServiceId, ServiceStatus

CHECK_TIMEOUT = 5


class CheckNotConfigured(Exception):
    pass


def check_batch_runtime(env: Environment, service_id: str) -> str:
    run = latest_run()
    verified_at = getattr(run, f"{service_id}_verified_at", None) if run else None

    if verified_at is None:
        failed_stages = {"etl"} if service_id == "etl" else {"spark", "verification"}

        if run and run.status == "failed" and run.stage in failed_stages:
            raise RuntimeError("Last pipeline stage failed")

        raise CheckNotConfigured("Run a complete integration to verify this service.")

    if service_id == "etl":
        if run is None or run.etl_engine != "apache-hop":
            raise CheckNotConfigured("Run an Apache Hop integration to verify this runtime.")
        java = str(Path(env.required("HOP_JAVA_HOME")) / "bin/java")
        job = Path(env.required("HOP_PROJECT_DIR")) / "workflows/integrate.hwf"
        if not job.is_file() or not (Path(env.required("HOP_HOME")) / "hop-run.sh").is_file():
            raise CheckNotConfigured("Apache Hop workflow is not installed.")
    else:
        import pyspark

        java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if os.environ.get("JAVA_HOME") else shutil.which("java")

        if not java or not pyspark.__version__:
            raise CheckNotConfigured("Spark runtime is not installed.")

    subprocess.run([java, "-version"], check=True, timeout=3, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    return f"Last successful verification: {verified_at.strftime('%Y-%m-%d %H:%M:%S UTC')}."


def check_spark(env: Environment) -> str:
    return check_batch_runtime(env, "spark")


def check_etl(env: Environment) -> str:
    return check_batch_runtime(env, "etl")


def check_oracle(env: Environment) -> None:
    from abassurance.systems.data_integration.extraction.oracle import connect_oracle

    port = env.integer("ORACLE_PORT", "1521", 65535)

    config = OracleConfig(
        user=env.get("ORACLE_USER", "ABASSURANCE"),
        password=env.required("ORACLE_APP_PASSWORD"),
        dsn=env.get("ORACLE_DSN")
        or f"{env.get('ORACLE_HOST', '127.0.0.1')}:{port}/{env.get('ORACLE_SERVICE', 'FREEPDB1')}",
        timeout=CHECK_TIMEOUT,
    )

    with connect_oracle(config) as connection:
        connection.call_timeout = CHECK_TIMEOUT * 1000

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM DUAL")
            row = cursor.fetchone()

            if row is None or row[0] != 1:
                raise ValueError("Unexpected Oracle response")


def check_sqlserver(env: Environment) -> None:
    import pyodbc

    from abassurance.systems.data_integration.extraction.sqlserver import connect_sqlserver

    config = replace(load_sqlserver_config(env), timeout=CHECK_TIMEOUT)

    if config.driver not in pyodbc.drivers():
        raise CheckNotConfigured("SQL Server ODBC driver is not installed.")

    with closing(connect_sqlserver(config=config)) as connection, closing(connection.cursor()) as cursor:
        cursor.execute("SELECT 1")
        row = cursor.fetchone()

        if row is None or row[0] != 1:
            raise ValueError("Unexpected SQL Server response")


def check_kafka(env: Environment) -> None:
    from confluent_kafka.admin import AdminClient

    client = AdminClient(
        {
            "bootstrap.servers": env.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            "socket.timeout.ms": CHECK_TIMEOUT * 1000,
            "socket.connection.setup.timeout.ms": CHECK_TIMEOUT * 1000,
            "log_level": 0,
        }
    )

    metadata = client.list_topics(timeout=CHECK_TIMEOUT)

    if not metadata.brokers:
        raise ValueError("No Kafka broker returned")


def check_hdfs(env: Environment) -> None:
    port = env.integer("HDFS_NAMENODE_UI_PORT", "9870", 65535)
    base_url = env.get("HDFS_HTTP_URL", f"http://127.0.0.1:{port}").rstrip("/")
    url = urlsplit(base_url)

    valid = (
        url.scheme in {"http", "https"}
        and url.hostname
        and not url.username
        and not url.password
        and not url.query
        and not url.fragment
    )

    if not valid:
        raise ConfigurationError("Invalid HDFS HTTP URL")
    if url.path:
        raise ConfigurationError("HDFS HTTP URL must not contain a path")

    with urlopen(f"{base_url}/webhdfs/v1/?op=GETFILESTATUS", timeout=CHECK_TIMEOUT) as response:
        if json.load(response)["FileStatus"]["type"] != "DIRECTORY":
            raise ValueError("Unexpected HDFS root")

    with urlopen(
        f"{base_url}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState", timeout=CHECK_TIMEOUT
    ) as response:
        beans = json.load(response)["beans"]

        if not any(bean.get("NumLiveDataNodes", 0) > 0 for bean in beans):
            raise ValueError("No live HDFS DataNode")


async def check_service(
    service_id: ServiceId, name: str, check: Callable[[Environment], str | None], env: Environment
) -> ServiceStatus:
    try:
        message = await asyncio.wait_for(asyncio.to_thread(check, env), timeout=CHECK_TIMEOUT)
        return ServiceStatus(id=service_id, name=name, status="available", message=message)

    except CheckNotConfigured:
        return ServiceStatus(
            id=service_id,
            name=name,
            status="not_checked",
            message="Service verification is not available. Check the server setup or run a complete integration.",
        )

    except (ImportError, ConfigurationError):
        return ServiceStatus(
            id=service_id,
            name=name,
            status="not_checked",
            message="Missing or invalid server configuration or dependency.",
        )

    except TimeoutError:
        return ServiceStatus(id=service_id, name=name, status="unavailable", message="Service health check timed out.")

    except Exception:
        return ServiceStatus(
            id=service_id, name=name, status="unavailable", message="Service is unreachable or its health check failed."
        )


async def get_services_status() -> list[ServiceStatus]:
    env = Environment.from_file(PROJECT_ROOT / ".env")

    checks: tuple[tuple[ServiceId, str, Callable[[Environment], str | None]], ...] = (
        ("oracle", "Oracle", check_oracle),
        ("sqlserver", "SQL Server", check_sqlserver),
        ("kafka", "Kafka", check_kafka),
        ("spark", "Spark", check_spark),
        ("hdfs", "Hadoop HDFS", check_hdfs),
        ("etl", "Apache Hop (ETL)", check_etl),
    )

    return list(
        await asyncio.gather(*(check_service(service_id, name, check, env) for service_id, name, check in checks))
    )
