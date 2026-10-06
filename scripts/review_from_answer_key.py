#!/usr/bin/env python3
"""Stand in for a data steward: decide every PENDING mapping of one country from its answer key.

Uses the same `review_store.record_decision` as the app, so the audit trail is identical, except that
`action_source` is ANSWER_KEY_SCRIPT and the reviewer name says so. The evidence discloses both.
"""

import argparse
from pathlib import Path

from databricks.sdk import WorkspaceClient

from harmonization import review_store
from harmonization.evaluation import load_answer_key

ROOT = Path(__file__).resolve().parents[1]


def decide(proposed: str | None, expected: str | None) -> tuple[str, dict]:
    """Return the review action a steward with the answer key would take."""
    if expected is None:
        return "REJECT", {"comment": "No group target for this local column."}
    if (proposed or "").casefold() == expected.casefold():
        return "APPROVE", {}
    return "CORRECT", {
        "final_global": expected,
        "final_match_type": "SEMANTIC_TRANSLATION",
        "comment": f"AI proposed {proposed}",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="pt")
    parser.add_argument("--country", required=True, choices=["ES", "IT"])
    parser.add_argument("--endpoint", default="projects/halvard-harmonization/branches/production/endpoints/primary")
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile)
    source_system, answer_key = load_answer_key(ROOT / "examples" / "answer_keys" / f"{args.country.lower()}.json")
    reviewer = f"answer-key-script ({w.current_user.me().user_name})"
    conn = review_store.connect(w, args.endpoint)
    try:
        counts: dict[str, int] = {}
        for row in review_store.fetch_queue(conn, source_system, "PENDING"):
            action, kwargs = decide(row["proposed_global_column_name"], answer_key.get(row["local_column_name"]))
            review_store.record_decision(
                conn, source_system, row["local_column_name"], action, reviewer, source="ANSWER_KEY_SCRIPT", **kwargs
            )
            counts[action] = counts.get(action, 0) + 1
            print(f"{action:8} {row['local_column_name']:32} AI={row['proposed_global_column_name']}")
    finally:
        conn.close()
    print(f"{source_system}: {counts}")


if __name__ == "__main__":
    main()
