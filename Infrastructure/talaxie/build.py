import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

API = "http://127.0.0.1:8787/api/v1"
OUTPUT = Path("/output")


def request(path, payload=None, content_type="application/json"):
    body = json.dumps(payload).encode() if isinstance(payload, dict) else payload
    query = urllib.request.Request(
        API + path, data=body, headers={"Content-Type": content_type}, method="POST" if payload is not None else "GET"
    )

    with urllib.request.urlopen(query, timeout=60) as response:
        return response.read()


def compile_job():
    archive = Path("/tmp/abassurance-items.zip")
    sources = Path("/sources/project")

    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as package:
        for path in sorted(sources.rglob("*")):
            if not path.is_file():
                continue

            if path.name == "AbAssuranceKafka_0.1.properties":
                definition = re.sub(r"\s*<imports\b[^>]*/>", "", path.read_text())
                package.writestr(path.relative_to(sources).as_posix(), definition)
            else:
                package.write(path, path.relative_to(sources))

    request("/projects/ABASSURANCE/import", archive.read_bytes(), "application/zip")

    build = json.loads(request("/projects/ABASSURANCE/jobs/ExtractPublish/builds", {"type": "standalone"}))
    seen = 0
    deadline = time.monotonic() + 600

    while time.monotonic() < deadline:
        status = json.loads(request(f"/builds/{build['id']}"))

        for step in status["steps"][seen:]:
            print(step, flush=True)

        seen = len(status["steps"])

        if status["status"] == "failed":
            raise RuntimeError(status["error"])

        if status["status"] == "done":
            export = Path("/tmp/ExtractPublish_0.1.zip")
            export.write_bytes(request(f"/builds/{build['id']}/archive"))

            OUTPUT.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(export) as package:
                for member in package.namelist():
                    if not (OUTPUT / member).resolve().is_relative_to(OUTPUT.resolve()):
                        raise RuntimeError("Invalid export path")

                package.extractall(OUTPUT)

            return

        time.sleep(1)

    raise TimeoutError("Talaxie compilation exceeded ten minutes")


def test_contract():
    classpath = os.pathsep.join(str(path) for path in OUTPUT.rglob("*.jar"))
    destination = "/tmp/contract-test"

    subprocess.run(
        [
            "javac",
            "--release",
            "8",
            "-proc:none",
            "-Xlint:-options",
            "-cp",
            classpath,
            "-d",
            destination,
            "/sources/tests/KafkaContractTest.java",
        ],
        check=True,
    )

    subprocess.run(["java", "-ea", "-cp", destination + os.pathsep + classpath, "KafkaContractTest"], check=True)


def configure_components():
    components = Path(os.environ["RUXIE_COMPONENTS_DIR"])
    oracle = components / "tOracleInput/tOracleInput_java.xml"

    definition = oracle.read_text()
    replacements = {
        'MODULE="ojdbc8-19.19.0.0.jar"': 'MODULE="ojdbc11-23.26.4.0.0.jar"',
        'MVN="mvn:com.oracle.database.jdbc/ojdbc8/19.19.0.0"': 'MVN="mvn:com.oracle.database.jdbc/ojdbc11/23.26.4.0.0"',
    }

    for original, replacement in replacements.items():
        if definition.count(original) != 1:
            raise RuntimeError("Unexpected Talaxie Oracle driver declaration")
        definition = definition.replace(original, replacement)

    oracle.write_text(definition)

    java_row = components / "tJavaRow/tJavaRow_java.xml"
    tree = ET.parse(java_row)
    code_generation = tree.getroot().find("CODEGENERATION")

    if code_generation is None:
        raise RuntimeError("Missing CODEGENERATION element in Talaxie tJavaRow component")

    imports = ET.SubElement(code_generation, "IMPORTS")

    for module, uri in (
        ("kafka-clients-4.1.2.jar", "mvn:org.apache.kafka/kafka-clients/4.1.2"),
        ("gson-2.13.2.jar", "mvn:com.google.code.gson/gson/2.13.2"),
    ):
        ET.SubElement(imports, "IMPORT", NAME=module, MODULE=module, MVN=uri, REQUIRED="true")

    tree.write(java_row, encoding="unicode")


def main():
    configure_components()
    Path("/workspace").mkdir(exist_ok=True)

    server = subprocess.Popen(
        ["/opt/talaxie/ruxie-server", "--port", "8787", "--workspace", "/workspace", "--lang", "en"]
    )

    try:
        for _ in range(60):
            try:
                health = json.loads(request("/health"))

                if health.get("version") != "9.0.2":
                    raise RuntimeError("Unexpected Talaxie compiler version")
                break

            except (urllib.error.URLError, ConnectionError) as exc:
                if server.poll() is not None:
                    raise RuntimeError("Talaxie compiler stopped before startup") from exc

                time.sleep(0.5)
        else:
            raise TimeoutError("Talaxie compiler did not start")

        compile_job()
        test_contract()

    finally:
        server.terminate()

        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


if __name__ == "__main__":
    main()
