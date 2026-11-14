import math
import random
from datetime import date, timedelta

from mapping.identifiers import make_id
from models.history import HISTORY_MODELS

GENERATOR_VERSION = 1
OBSERVATIONS = ("2025-07-01", "2026-01-01")


def generation_recipe(contracts_per_source: int, seed: int) -> dict:
    return {
        "profile": "predictive-behaviour",
        "version": GENERATOR_VERSION,
        "seed": seed,
        "contracts_per_source_per_cohort": contracts_per_source,
        "observation_dates": list(OBSERVATIONS),
        "horizon_days": 90,
        "outcome_mechanism": "Past behaviour -> noisy logistic hazard -> future Bernoulli outcome",
        "business_calibrated": False,
    }


def predictive_records(contracts_per_source: int = 52, *, seed: int = 42) -> dict[str, list[dict]]:
    if type(contracts_per_source) is not int or not 13 <= contracts_per_source <= 10000:
        raise ValueError("contracts-per-source must be between 13 and 10000")

    if type(seed) is not int or not 0 <= seed < 2**31:
        raise ValueError("seed must be an integer in [0, 2**31)")

    records = {entity: [] for entity in HISTORY_MODELS}

    for cohort, day in enumerate(OBSERVATIONS):
        observation = date.fromisoformat(day)
        horizon_end = observation + timedelta(days=90)

        for source_index, source in enumerate(("abassurance", "assureplus")):
            behaviour = random.Random(seed + 1009 * cohort + 7919 * source_index)
            outcomes = random.Random(seed + 104729 + 1009 * cohort + 7919 * source_index)

            common = {"source_system": source, "revision": 1, "deleted": False, "is_synthetic": True}

            customer_stress = 0.0

            for index in range(contracts_per_source):
                prefix = f"PRED-V{GENERATOR_VERSION}-S{seed}-COHORT{cohort}"
                reference = f"{prefix}-C{index:05d}"
                customer_ref = f"{prefix}-U{index // 2:05d}"
                contract_id = make_id(source, reference)

                if index % 2 == 0:
                    customer_stress = behaviour.random()

                    records["customers"].append(
                        {
                            **common,
                            "customer_id": make_id(source, customer_ref),
                            "source_id": customer_ref,
                            "available_at": f"{observation - timedelta(days=1500)}T00:00:00Z",
                        }
                    )

                start = observation - timedelta(days=behaviour.randint(365, 1300))

                records["contracts"].extend(
                    [
                        {
                            **common,
                            "contract_id": contract_id,
                            "source_reference": reference,
                            "customer_id": make_id(source, customer_ref),
                            "start_date": str(start),
                            "coverage_end_date": None,
                            "available_at": f"{start}T00:00:00Z",
                        },
                        {
                            **common,
                            "revision": 2,
                            "contract_id": contract_id,
                            "source_reference": reference,
                            "customer_id": make_id(source, customer_ref),
                            "start_date": str(start),
                            "coverage_end_date": str(horizon_end),
                            "available_at": f"{horizon_end + timedelta(days=1)}T00:00:00Z",
                        },
                    ]
                )

                recency = behaviour.randint(5, 35) if customer_stress < 0.55 else behaviour.randint(35, 130)

                payment_count = behaviour.randint(4, 7) if customer_stress < 0.55 else behaviour.randint(1, 5)

                premium = behaviour.randint(70, 180)

                for number in range(payment_count):
                    paid = observation - timedelta(days=recency + 28 * number)
                    payment_ref = f"{reference}-PAY{number}"

                    records["payments"].append(
                        {
                            **common,
                            "payment_id": make_id(source, payment_ref),
                            "source_id": payment_ref,
                            "contract_id": contract_id,
                            "payment_date": str(paid),
                            "amount": None if behaviour.random() < 0.08 else f"{premium}.00",
                            "available_at": f"{paid + timedelta(days=1)}T00:00:00Z",
                        }
                    )

                claim_count = sum(behaviour.random() < 0.15 + 0.35 * customer_stress for _ in range(5))

                for number in range(claim_count):
                    incident = observation - timedelta(days=behaviour.randint(10, 180))
                    claim_ref = f"{reference}-CLAIM{number}"

                    records["claims"].append(
                        {
                            **common,
                            "claim_id": make_id(source, claim_ref),
                            "source_id": claim_ref,
                            "contract_id": contract_id,
                            "incident_date": str(incident),
                            "available_at": f"{incident + timedelta(days=2)}T00:00:00Z",
                        }
                    )

                records["followup"].append(
                    {
                        **common,
                        "contract_id": contract_id,
                        "complete_from": str(start),
                        "features_complete_from": str(start),
                        "complete_until": str(horizon_end),
                        "available_at": f"{horizon_end + timedelta(days=1)}T00:00:00Z",
                    }
                )

                log_odds = (
                    -3.2
                    + 0.035 * (recency - 20)
                    + 0.55 * claim_count
                    + 0.3 * (5 - payment_count)
                    + outcomes.gauss(0, 0.65)
                )

                probability = min(0.93, max(0.03, 1 / (1 + math.exp(-log_odds))))

                if outcomes.random() < probability:
                    effective = observation + timedelta(days=outcomes.randint(5, 85))
                    cancellation_ref = f"{reference}-EXIT"

                    records["cancellations"].append(
                        {
                            **common,
                            "cancellation_id": make_id(source, cancellation_ref),
                            "source_id": cancellation_ref,
                            "contract_id": contract_id,
                            "cancellation_date": str(effective),
                            "cancellation_type": "voluntary",
                            "available_at": f"{effective + timedelta(days=1)}T00:00:00Z",
                        }
                    )

    return records
