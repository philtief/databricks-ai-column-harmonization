#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient

from harmonization.genie_space import load_space_config, render_benchmark


CONFIG_PATH = Path(__file__).resolve().parents[1] / "genie" / "space_config.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Genie question benchmark")
    parser.add_argument("--profile", default="pt")
    parser.add_argument("--space-id", required=True)
    parser.add_argument("--questions", action="append", help="Repeat for custom questions")
    parser.add_argument("--out", default="evidence/genie_benchmark.md")
    return parser.parse_args()


def _first_value(value: Any, *path: str) -> Any:
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _parse_message(response: dict[str, Any]) -> tuple[str, str]:
    message = response.get("message", response)
    return response.get("conversation_id") or _first_value(message, "conversation_id") or "", message.get("id") or ""


def _attachment_fields(attachment: dict[str, Any]) -> tuple[str, str, str]:
    text = _first_value(attachment, "text", "content") or attachment.get("text") or attachment.get("content")
    query = attachment.get("query") or {}
    description = query.get("description")
    sql = query.get("query")
    if not sql:
        sql = _first_value(attachment, "query", "sql")
    return str(text or ""), str(description or ""), str(sql or "")


def _parse_rows(response: dict[str, Any]) -> tuple[list[str], list[list[Any]]]:
    result = response.get("result") or response.get("statement_response", {}).get("result") or {}
    data = result.get("data") or []
    manifest = response.get("manifest") or response.get("statement_response", {}).get("manifest") or {}
    schema = manifest.get("schema", {})
    columns = [column["name"] for column in schema.get("columns", [])]
    rows = [row if isinstance(row, list) else list(row) for row in data]
    return columns, rows[:10]


def ask_question(workspace: WorkspaceClient, space_id: str, question: str) -> dict[str, Any]:
    try:
        if hasattr(workspace, "genie") and hasattr(workspace.genie, "start_conversation_and_wait"):
            message = workspace.genie.start_conversation_and_wait(space_id, question)
            response = message.model_dump(mode="json") if hasattr(message, "model_dump") else {"message": message}
            conversation_id = response.get("conversation_id") or ""
            message_id = response.get("id") or ""
            attachments = response.get("attachments") or []
        else:
            response = workspace.api_client.do(
                "POST",
                f"/api/2.0/genie/spaces/{space_id}/start-conversation",
                body={"content": question},
            )
            conversation_id, message_id = _parse_message(response)
            attachments = response.get("attachments") or []
            if not attachments and conversation_id and message_id:
                attachments = workspace.api_client.do(
                    "GET",
                    f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments",
                )
                if isinstance(attachments, dict):
                    attachments = attachments.get("attachments", [])

        answer_parts = []
        description = None
        sql = None
        columns = []
        rows = []
        for attachment in attachments:
            text, attachment_description, attachment_sql = _attachment_fields(attachment)
            if text:
                answer_parts.append(text)
            if attachment_description and description is None:
                description = attachment_description
            if attachment_sql and sql is None:
                sql = attachment_sql
                attachment_id = attachment.get("attachment_id") or attachment.get("id")
                if attachment_id and conversation_id and message_id:
                    result_response = workspace.api_client.do(
                        "GET",
                        f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result",
                    )
                    columns, rows = _parse_rows(result_response)

        return {
            "question": question,
            "status": "SUCCESS",
            "answer": "\n\n".join(answer_parts) or None,
            "sql": sql,
            "description": description,
            "columns": columns,
            "rows": rows,
            "error": None,
        }
    except Exception as error:
        return {
            "question": question,
            "status": "ERROR",
            "answer": None,
            "sql": None,
            "description": None,
            "columns": [],
            "rows": [],
            "error": str(error),
        }


def main() -> int:
    args = parse_args()
    cfg = load_space_config(CONFIG_PATH)
    questions = args.questions or cfg["sample_questions"]
    workspace = WorkspaceClient(profile=args.profile)
    results = []
    for question in questions:
        result = ask_question(workspace, args.space_id, question)
        result["space_id"] = args.space_id
        results.append(result)
    rendered = render_benchmark(results)
    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(f"answered with SQL: {sum(1 for result in results if result['sql'])}/{len(results)}")
    print(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
