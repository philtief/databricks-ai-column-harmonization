#!/usr/bin/env python3
"""Ask the Genie space its sample questions and write the answers, SQL and rows as markdown evidence."""

import argparse
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient

from harmonization.genie_space import load_space_config, render_benchmark

CONFIG_PATH = Path(__file__).resolve().parents[1] / "genie" / "space_config.yaml"


def ask_question(w: WorkspaceClient, space_id: str, question: str) -> dict[str, Any]:
    result = {"question": question, "space_id": space_id, "status": "SUCCESS", "answer": None, "sql": None}
    result.update(description=None, columns=[], rows=[], error=None)
    try:
        message = w.genie.start_conversation_and_wait(space_id, question)
        answers = []
        for attachment in message.attachments or []:
            if attachment.text and attachment.text.content:
                answers.append(attachment.text.content)
            if attachment.query and result["sql"] is None:
                result["sql"] = attachment.query.query
                result["description"] = attachment.query.description
                statement = w.genie.get_message_attachment_query_result(
                    space_id, message.conversation_id, message.message_id, attachment.attachment_id
                ).statement_response
                if statement and statement.result and statement.manifest:
                    result["columns"] = [column.name for column in statement.manifest.schema.columns]
                    result["rows"] = (statement.result.data_array or [])[:10]
        result["answer"] = "\n\n".join(answers) or None
    except Exception as error:  # one failing question must not stop the benchmark
        result.update(status="ERROR", error=str(error))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="pt")
    parser.add_argument("--space-id", required=True)
    parser.add_argument("--questions", action="append", help="Repeat for custom questions")
    parser.add_argument("--out", default="evidence/genie_benchmark.md")
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile)
    questions = args.questions or load_space_config(CONFIG_PATH)["sample_questions"]
    results = [ask_question(w, args.space_id, question) for question in questions]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_benchmark(results), encoding="utf-8")
    print(f"answered with SQL: {sum(1 for r in results if r['sql'])}/{len(results)} -> {out}")


if __name__ == "__main__":
    main()
