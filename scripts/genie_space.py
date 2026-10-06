#!/usr/bin/env python3
"""Create or update the Halvard Genie space from genie/space_config.yaml."""

import argparse
from pathlib import Path

from databricks.sdk import WorkspaceClient

from harmonization.genie_space import load_space_config, to_api_payload

CONFIG_PATH = Path(__file__).resolve().parents[1] / "genie" / "space_config.yaml"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="pt")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--space-id", help="Update this space instead of creating one")
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile)
    payload = to_api_payload(
        load_space_config(CONFIG_PATH),
        args.catalog,
        args.schema,
        args.warehouse_id,
        parent_path=f"/Users/{w.current_user.me().user_name}",
    )
    space = w.genie.update_space(args.space_id, **payload) if args.space_id else w.genie.create_space(**payload)
    print(space.space_id)
    print(f"{w.config.host.rstrip('/')}/genie/rooms/{space.space_id}")


if __name__ == "__main__":
    main()
