from datetime import date, datetime

import pytest
from abassurance.systems.data_preparation import predictive
from abassurance.systems.data_preparation.predictive import OBSERVATIONS, generation_recipe, predictive_records


def test_predictive_generator_is_reproducible_independent_and_explicitly_synthetic():
    records = predictive_records()

    assert records == predictive_records()
    assert records != predictive_records(seed=43)
    assert generation_recipe(52, 42)["business_calibrated"] is False
    assert all(row["is_synthetic"] for rows in records.values() for row in rows)

    cohorts = []

    for cohort, day in enumerate(OBSERVATIONS):
        marker = f"COHORT{cohort}-"
        customers = {r["customer_id"] for r in records["customers"] if marker in r["customer_id"]}

        assert len(customers) == 52

        cohorts.append(customers)
        observation = date.fromisoformat(day)

        for entity, field in (("payments", "payment_date"), ("claims", "incident_date")):
            rows = [r for r in records[entity] if marker in r["contract_id"]]

            assert all(date.fromisoformat(r[field]) < observation for r in rows)
            assert all(datetime.fromisoformat(r["available_at"]).date() < observation for r in rows)

        exits = [r for r in records["cancellations"] if marker in r["contract_id"]]

        assert 0 < len(exits) < 104
        assert all(date.fromisoformat(r["cancellation_date"]) > observation for r in exits)
        assert all(datetime.fromisoformat(r["available_at"]).date() > observation for r in exits)

    assert cohorts[0].isdisjoint(cohorts[1])


def test_changing_future_outcome_draws_cannot_change_past_features(monkeypatch):
    original = predictive_records()
    random_class = predictive.random.Random

    def change_outcomes_only(seed):
        return random_class(seed + 300000 if seed >= 104729 else seed)

    monkeypatch.setattr(predictive.random, "Random", change_outcomes_only)
    changed = predictive_records()

    for entity in ("customers", "contracts", "payments", "claims", "followup"):
        assert changed[entity] == original[entity]

    assert changed["cancellations"] != original["cancellations"]


@pytest.mark.parametrize("options", [{"contracts_per_source": 0}, {"seed": -1}, {"seed": True}])
def test_predictive_generator_rejects_invalid_configuration(options):
    with pytest.raises(ValueError):
        predictive_records(**options)
