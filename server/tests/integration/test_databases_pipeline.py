import json
from collections import Counter
from contextlib import closing
from dataclasses import fields
from decimal import Decimal

import pytest
from abassurance.shared.models import (
    Claim,
    Contract,
    Customer,
    OracleClaim,
    OracleContract,
    OracleCustomer,
    OraclePayment,
    Payment,
    SQLServerClaim,
    SQLServerContract,
    SQLServerCustomer,
    SQLServerPayment,
)
from abassurance.systems.data_integration.extraction.config import load_settings
from abassurance.systems.data_integration.extraction.oracle import connect_oracle, fetch_oracle
from abassurance.systems.data_integration.extraction.sqlserver import connect_sqlserver, fetch_sqlserver
from abassurance.systems.data_integration.pipeline.run import run_pipeline
from abassurance.systems.data_integration.pipeline.validation import ID_FIELDS

pytestmark = pytest.mark.integration
MODELS = {"customers": Customer, "contracts": Contract, "claims": Claim, "payments": Payment}
SOURCE_MODELS = {
    "abassurance": dict(zip(MODELS, [OracleCustomer, OracleContract, OracleClaim, OraclePayment], strict=True)),
    "assureplus": dict(
        zip(MODELS, [SQLServerCustomer, SQLServerContract, SQLServerClaim, SQLServerPayment], strict=True)
    ),
}


@pytest.fixture(scope="module")
def live_settings():
    return load_settings()


@pytest.fixture(scope="module")
def raw_data(live_settings):
    return {
        "abassurance": {entity: fetch_oracle(entity, live_settings.oracle) for entity in MODELS},
        "assureplus": {entity: fetch_sqlserver(entity, live_settings.sqlserver) for entity in MODELS},
    }


@pytest.fixture(scope="module")
def exports(live_settings, tmp_path_factory):
    folder = tmp_path_factory.mktemp("integration-exports")
    return folder, run_pipeline(folder, settings=live_settings)


@pytest.mark.parametrize("source", ["abassurance", "assureplus"])
def test_live_connection(live_settings, source):
    connection = (
        connect_oracle(live_settings.oracle)
        if (source == "abassurance")
        else connect_sqlserver(config=live_settings.sqlserver)
    )
    query = "SELECT 1 FROM dual" if (source == "abassurance") else "SELECT 1"
    with closing(connection), closing(connection.cursor()) as cursor:
        cursor.execute(query)
        row = cursor.fetchone()

        assert row is not None
        assert row[0] == 1


@pytest.mark.parametrize("source", ["abassurance", "assureplus"])
@pytest.mark.parametrize("entity", MODELS)
def test_annex_tables_and_demo_records(raw_data, source, entity):
    rows = raw_data[source][entity]
    assert len(rows) >= 2, f"Initialize fictitious data for {source}/{entity}"
    expected = set(SOURCE_MODELS[source][entity].__annotations__)
    assert all(set(row) == expected for row in rows)


@pytest.mark.parametrize("entity", MODELS)
def test_json_shape_totals_and_provenance(raw_data, exports, entity):
    folder, data = exports
    rows = data[entity]
    exported = json.loads((folder / f"{entity}.json").read_text("utf-8"))

    assert exported == rows
    assert Counter(row["source_system"] for row in rows) == {
        source: len(entities[entity]) for source, entities in raw_data.items()
    }
    assert all(set(row) == {field.name for field in fields(MODELS[entity])} for row in rows)
    assert len({row[ID_FIELDS[entity]] for row in rows}) == len(rows)

    original_field = "source_reference" if (entity == "contracts") else "source_id"
    for source, entities in raw_data.items():
        primary_key = next(iter(SOURCE_MODELS[source][entity].__annotations__))
        assert {str(row[primary_key]) for row in entities[entity]} == {
            row[original_field] for row in rows if (row["source_system"] == source)
        }


def test_driver_types_and_fictional_transformations(raw_data, exports):
    assert isinstance(raw_data["abassurance"]["contracts"][0]["AB_PRIME_ANNUELLE"], Decimal)
    assert isinstance(raw_data["assureplus"]["contracts"][0]["AP_MONTHLY_PREMIUM"], Decimal)
    assert all(isinstance(row["AB_DESCRIPTION"], str) for row in raw_data["abassurance"]["claims"])
    _, data = exports

    customers = {row["customer_id"]: row for row in data["customers"]}
    assert customers["0ce23490-7b17-57a3-933d-829abb01a605"]["full_name"] == "Jean Dupont"
    assert customers["3fff094d-64af-55c0-abc9-2e86a559959e"]["full_name"] == "Marie Martin"
    assert customers["0ce23490-7b17-57a3-933d-829abb01a605"]["source_id"] == customers["3fff094d-64af-55c0-abc9-2e86a559959e"]["source_id"] == "100"
    assert customers["0ce23490-7b17-57a3-933d-829abb01a605"]["birth_date"] == "1990-05-12"

    contracts = {row["contract_id"]: row for row in data["contracts"]}
    assert contracts["208bea4b-6ddd-56d6-921a-ee89bfd4a4e7"]["annual_premium"] == "1200.00"
    assert contracts["30eadb86-10c5-5322-a97d-68a205673434"]["annual_premium"] == "1200.00"

    claims = {row["claim_id"]: row for row in data["claims"]}
    assert claims["a5207183-f805-58b9-8a41-e30d5670552b"]["incident_date"] == "2025-03-10T14:30:00"
    assert claims["a5207183-f805-58b9-8a41-e30d5670552b"]["fraud_score"] == "12.50"


def test_relationships_stay_in_the_same_company(exports):
    _, data = exports
    for child, parent, key in (
        ("contracts", "customers", "customer_id"),
        ("claims", "contracts", "contract_id"),
        ("payments", "contracts", "contract_id"),
    ):
        parents = {row[ID_FIELDS[parent]]: row for row in data[parent]}
        for row in data[child]:
            assert row[key] in parents
            assert parents[row[key]]["source_system"] == row["source_system"]
