#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

from harmonization.genie_space import load_space_config, to_api_payload


CONFIG_PATH = Path(__file__).resolve().parents[1] / "genie" / "space_config.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update the Halvard Genie space")
    parser.add_argument("--profile", default="pt")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--space-id")
    parser.add_argument("--out", help="Write the created space id to this file")
    return parser.parse_args()


def extract_space_id(response: dict | str, fallback_id: str | None = None) -> str:
    if isinstance(response, str):
        return fallback_id or ""
    return response.get("space_id") or response.get("id") or fallback_id or ""


def main() -> int:
    args = parse_args()
    cfg = load_space_config(CONFIG_PATH)
    workspace = WorkspaceClient(profile=args.profile)
    payload = to_api_payload(cfg, args.catalog, args.schema, args.warehouse_id, parent_path="/Users")
    if parent_path := workspace.current_user.me().user_name:
        payload["parent_path"] = f"/Users/{parent_path}"

    if args.space_id:
        response = workspace.api_client.do("PATCH", f"/api/2.0/genie/spaces/{args.space_id}", body=payload)
        space_id = extract_space_id(response, args.space_id)
    elif hasattr(workspace, "genie") and hasattr(workspace.genie, "create_space"):
        space = workspace.genie.create_space(**payload)
        space_id = extract_space_id(space.model_dump() if hasattr(space, "model_dump") else space)
    else:
        response = workspace.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)
        space_id = extract_space_id(response)

    if not space_id:
        raise RuntimeError("Databricks did not return a space id")
    if args.out:
        Path(args.out).write_text(space_id, encoding="utf-8")

    host = workspace.config.host.rstrip("/")
    print(space_id)
    print(f"{host}/genie/spaces/{space_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
