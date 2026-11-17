import pytest
from abassurance.systems.data_integration.mapping import map_claim, map_contract, map_customer, map_payment
from abassurance.systems.data_integration.mapping.normalizers import CATEGORIES, normalize_category


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ("customer_status", {"ACTIVE", "INACTIVE"}),
        ("contract_status", {"ACTIVE", "CANCELLED", "EXPIRED", "SUSPENDED"}),
        ("claim_status", {"OPEN", "CLOSED", "IN_PROGRESS", "REJECTED"}),
        ("payment_method", {"BANK_TRANSFER", "CARD", "DIRECT_DEBIT", "CHECK", "CASH"}),
        ("transaction_status", {"COMPLETED", "PENDING", "FAILED", "REFUNDED", "CANCELLED"}),
        ("product_type", {"CAR", "HOME", "HEALTH", "LIFE"}),
    ],
)
def test_common_codes_are_exclusively_english(field, expected):
    assert set(CATEGORIES[field].values()) == expected


@pytest.mark.parametrize(("field", "aliases"), CATEGORIES.items())
def test_all_category_aliases_are_canonical_and_idempotent(field, aliases):
    for original, expected in aliases.items():
        assert normalize_category(original.lower(), field) == expected
        assert normalize_category(f" {original} ", field) == expected
        assert normalize_category(expected, field) == expected
    assert normalize_category(None, field) is None
    assert normalize_category("  ", field) is None
    with pytest.raises(ValueError):
        normalize_category("unmapped-new-value", field)
    with pytest.raises(ValueError):
        normalize_category(1, field)


def test_both_sources_produce_the_same_categories_without_changing_the_original():
    oracle = {"AB_CLIENT_ID": 100, "AB_NOM": "Nom", "AB_PRENOM": "Prenom", "AB_STATUT_CLIENT": "ACTIF"}
    sqlserver = {"AP_USER_ID": 100, "AP_CUSTOMER_STATUS": "ACTIVE"}
    assert (
        map_customer(oracle, "abassurance")["customer_status"]
        == map_customer(sqlserver, "assureplus")["customer_status"]
        == "ACTIVE"
    )
    assert sqlserver["AP_CUSTOMER_STATUS"] == "ACTIVE"
    assert (
        map_contract(
            {"AP_CONTRACT_REF": "1", "AP_USER_ID": 100, "AP_CONTRACT_STATE": "ACTIVE", "AP_PRODUCT_CODE": "HOME"},
            "assureplus",
        )["product_type"]
        == "HOME"
    )
    assert (
        map_claim({"AP_SINISTRE_NUM": 1, "AP_CONTRACT_REF": "1", "AP_CLAIM_STATUS": "CLOSED"}, "assureplus")[
            "claim_status"
        ]
        == "CLOSED"
    )
    assert (
        map_payment({"AP_PAYMENT_REF": "1", "AP_CONTRACT_REF": "1", "AP_PAYMENT_CHANNEL": "CARD"}, "assureplus")[
            "payment_method"
        ]
        == "CARD"
    )


def test_accents_and_separators_are_normalized():
    assert normalize_category("résilié", "contract_status") == "CANCELLED"
    assert normalize_category("bank-transfer", "payment_method") == "BANK_TRANSFER"
