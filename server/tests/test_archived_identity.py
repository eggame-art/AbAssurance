from copy import deepcopy

import pytest
from abassurance.systems.data_api.consolidated import validate_archived_identity
from abassurance.systems.data_integration.mapping.claims import CLAIM_FIELDS
from abassurance.systems.data_integration.mapping.contracts import CONTRACT_FIELDS
from abassurance.systems.data_integration.mapping.identifiers import SOURCE_PREFIXES, make_id
from abassurance.systems.data_integration.mapping.payments import PAYMENT_FIELDS
from abassurance.systems.data_integration.pipeline.validation import ID_FIELDS
from abassurance.systems.messaging.config import SOURCE_TABLES


@pytest.fixture(params=[(source, entity) for source in SOURCE_TABLES for entity in ID_FIELDS])
def archive(request):
    source, entity = request.param
    primary_field = SOURCE_TABLES[source][entity][1]

    data = {"source_system": source, ID_FIELDS[entity]: make_id(source, "100", entity)}

    raw = {primary_field: "100"}

    if entity == "contracts":
        customer_field = CONTRACT_FIELDS[source]["customer_id"]

        raw[customer_field] = "200"
        data["customer_id"] = make_id(source, "200", "customers")

    elif entity in ("claims", "payments"):
        fields = CLAIM_FIELDS if entity == "claims" else PAYMENT_FIELDS
        contract_field = fields[source]["contract"]

        raw[contract_field] = "C-200"
        data["contract_id"] = make_id(source, "C-200", "contracts")

    envelope = {"source_system": source, "entity": entity, "data": raw}

    return data, envelope, entity


def test_archive_identity_matches_the_returned_record(archive):
    data, envelope, entity = archive

    validate_archived_identity(data, envelope, entity)


def test_archive_of_another_record_is_refused(archive):
    data, envelope, entity = archive

    primary_field = SOURCE_TABLES[data["source_system"]][entity][1]
    envelope["data"][primary_field] = "another-customer-record"

    with pytest.raises(ValueError, match="Archived source identity"):
        validate_archived_identity(data, envelope, entity)


def test_archive_with_another_parent_is_refused(archive):
    data, envelope, entity = archive

    if entity == "customers":
        pytest.skip("Customers have no parent reference")

    parent_field = "customer_id" if entity == "contracts" else "contract_id"
    parent_entity = "customers" if entity == "contracts" else "contracts"

    data[parent_field] = make_id(data["source_system"], "foreign-parent", parent_entity)

    with pytest.raises(ValueError, match="Archived source identity"):
        validate_archived_identity(data, envelope, entity)


def test_archive_of_another_source_or_entity_is_refused(archive):
    data, envelope, entity = archive

    changes = [{"source_system": "unrelated"}, {"entity": "unrelated"}, {"data": []}]

    for change in changes:
        with pytest.raises(ValueError, match="Archived source"):
            validate_archived_identity(data, {**envelope, **change}, entity)


def test_legacy_identity_remains_supported(archive):
    data, envelope, entity = archive

    legacy = deepcopy(data)
    prefix = SOURCE_PREFIXES[data["source_system"]]

    legacy[ID_FIELDS[entity]] = f"{prefix}-100"

    validate_archived_identity(legacy, envelope, entity)


@pytest.mark.parametrize("envelope", [None, [], "private customer data"])
def test_invalid_archive_fails_without_exposing_its_content(envelope):
    with pytest.raises(ValueError, match="Archived source") as captured:
        validate_archived_identity({"source_system": "abassurance"}, envelope, "customers")

    assert "private customer data" not in str(captured.value)
