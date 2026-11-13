import json
from datetime import datetime

import pytest

from messaging.config import SOURCE_TABLES
from messaging.producer import _json_value


@pytest.fixture(scope="session")
def spark(request):
    pytest.importorskip("pyspark", reason="Install .[spark] with Python 3.11 and Java 17, or use Docker")

    from spark_pipeline.run import create_session

    session = create_session(kafka=request.config.getoption("--run-spark-kafka-integration"))
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture
def demo_sources():
    sources = {
        "abassurance": {entity: [] for entity in SOURCE_TABLES["abassurance"]},
        "assureplus": {entity: [] for entity in SOURCE_TABLES["assureplus"]},
    }

    for identifier in (100, 101):
        reference = f"C-{identifier}"

        sources["abassurance"]["customers"].append(
            {
                "AB_CLIENT_ID": identifier,
                "AB_PRENOM": "Jean",
                "AB_NOM": "Dupont",
                "AB_DATE_NAISSANCE": "1990-05-12",
                "AB_DATE_CREATION": "2025-01-15 10:30:00",
            }
        )

        sources["abassurance"]["contracts"].append(
            {
                "AB_POLICY_NUMBER": reference,
                "AB_CLIENT_ID": identifier,
                "AB_PRIME_ANNUELLE": "1199.88",
                "AB_DATE_DEBUT": "2025-01-01",
                "AB_DATE_FIN": "2025-12-31",
                "AB_AGENCE_ID": 1,
            }
        )

        sources["abassurance"]["claims"].append(
            {"AB_CLAIM_ID": identifier, "AB_POLICY_NUMBER": reference, "AB_MONTANT_ESTIME": "2500.00"}
        )

        sources["abassurance"]["payments"].append(
            {"AB_PAYMENT_ID": identifier, "AB_POLICY_NUMBER": reference, "AB_MONTANT": "99.99"}
        )

        sources["assureplus"]["customers"].append(
            {"AP_USER_ID": identifier, "AP_FULL_NAME": "Marie Martin", "AP_LOYALTY_SCORE": "80"}
        )

        sources["assureplus"]["contracts"].append(
            {
                "AP_CONTRACT_REF": reference,
                "AP_USER_ID": identifier,
                "AP_MONTHLY_PREMIUM": "99.99",
                "AP_START_DATE": datetime(2025, 1, 1),
                "AP_END_DATE": datetime(2025, 12, 31),
            }
        )

        sources["assureplus"]["claims"].append(
            {
                "AP_SINISTRE_NUM": identifier,
                "AP_CONTRACT_REF": reference,
                "AP_FRAUD_SCORE": "12.50",
                "AP_INCIDENT_DATE": "2025-03-10 14:30:00",
            }
        )

        sources["assureplus"]["payments"].append(
            {
                "AP_PAYMENT_REF": f"00000000-0000-0000-0000-{identifier:012d}",
                "AP_CONTRACT_REF": reference,
                "AP_PAYMENT_DATETIME": "2025-01-05 10:00:00",
                "AP_AMOUNT_PAID": "99.99",
            }
        )

    return sources


@pytest.fixture
def envelopes():
    def make(sources, *, prefix="insurance", copies=1):
        result = []

        for copy in range(copies):
            for source, entities in sources.items():
                for entity in reversed(entities):
                    primary_key = SOURCE_TABLES[source][entity][1]

                    for index, record in enumerate(entities[entity]):
                        result.append(
                            {
                                "key": f"{source}:{entity}:{record[primary_key]}",
                                "value": json.dumps(
                                    {"source_system": source, "entity": entity, "data": record}, default=_json_value
                                ),
                                "topic": f"{prefix}.{source}.{entity}",
                                "partition": 0,
                                "offset": copy * 100 + index,
                                "timestamp": datetime(2025, 1, 1),
                            }
                        )

        return result

    return make
