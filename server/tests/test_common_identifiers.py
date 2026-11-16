from uuid import UUID

import pytest
from abassurance.systems.data_integration.mapping.identifiers import make_id, matches_identity


def test_common_id_is_stable_scoped_uuid_with_a_fixed_reference():
    assert make_id("abassurance", 100, "customers") == "0ce23490-7b17-57a3-933d-829abb01a605"
    assert make_id("abassurance", 100, "customers") == make_id("abassurance", "100", "customers")
    identifiers = {
        make_id(source, 100, entity)
        for source in ("abassurance", "assureplus")
        for entity in ("customers", "contracts", "claims", "payments")
    }
    assert len(identifiers) == 8
    assert all(UUID(identifier).version == 5 for identifier in identifiers)


def test_old_id_is_only_accepted_when_reading_legacy_data():
    assert not matches_identity("abassurance", 100, "AB-100", "customers")
    assert matches_identity("abassurance", 100, "AB-100", "customers", allow_legacy=True)
    assert not matches_identity("assureplus", 100, "AB-100", "customers", allow_legacy=True)


@pytest.mark.parametrize("value", [None, "", "  "])
def test_missing_source_id_cannot_generate_a_uuid(value):
    with pytest.raises(ValueError):
        make_id("abassurance", value, "customers")
