import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "review_from_answer_key", Path(__file__).resolve().parents[1] / "scripts" / "review_from_answer_key.py"
)
script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(script)


def test_decide_follows_the_answer_key():
    assert script.decide("Region", "region") == ("APPROVE", {})
    action, kwargs = script.decide("loss_ratio", "gross_claims_incurred_eur")
    assert action == "CORRECT" and kwargs["final_global"] == "gross_claims_incurred_eur"
    action, kwargs = script.decide("record_id", None)
    assert action == "REJECT" and kwargs["comment"]
