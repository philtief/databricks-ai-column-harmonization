# WP3: Lakebase review store (psycopg 3)

## Goal
The operational store for the human review workflow lives in Lakebase Postgres. Build a small,
well-tested data-access module that the job notebooks and the Streamlit app share.

## Files
- NEW `src/harmonization/review_store.py`
- NEW `tests/test_review_store.py`
- EDIT `pyproject.toml`: add `psycopg[binary]>=3.2` to a new optional group `lakebase` and to `dev`.
  Then run `uv pip install -p .venv/bin/python -e ".[dev]"` so the tests can run.

## API (exact, see PLAN.md section 3 for the DDL)
```python
SCHEMA = "harmonization_review"
ACTIONS = {"APPROVE", "CORRECT", "REJECT", "RESET"}
def ensure_schema(conn) -> None                      # CREATE SCHEMA/TABLE IF NOT EXISTS + index on review_status
def upsert_queue(conn, rows: list[dict]) -> int      # INSERT ... ON CONFLICT (source_system, local_column_name)
    # DO UPDATE AI fields (local_data_type, local_sample_values, proposed_*, mapping_rationale, confidence,
    # mandatory_flag, mapping_version, published_at, updated_at) ONLY WHERE review_queue.review_status = 'PENDING'.
    # Returns rows inserted or updated. Sample values are serialised as JSON.
def fetch_queue(conn, source_system: str | None = None, status: str | None = None) -> list[dict]
def fetch_decisions(conn, source_system: str) -> list[dict]   # review_status <> 'PENDING'
def status_summary(conn) -> list[dict]               # source_system, review_status, mandatory_flag, count
def record_decision(conn, source_system, local_column_name, action, user, final_global=None,
                    final_match_type=None, comment="", source="DATABRICKS_APP") -> bool
    # validates: action in ACTIONS; REJECT needs non-empty comment; CORRECT needs final_global.
    # APPROVE: status APPROVED, final_global = proposed, final_match_type = proposed.
    # CORRECT: status CORRECTED, final_* from args. REJECT: status REJECTED. RESET: back to PENDING, final_* NULL.
    # Single transaction: SELECT ... FOR UPDATE, UPDATE queue, INSERT audit (old/new status and target).
    # Returns False if the row does not exist. Raises ValueError for validation errors.
def connect(workspace_client, endpoint_path: str, host: str | None = None,
            dbname: str = "databricks_postgres", user: str | None = None):
    # host defaults to workspace_client.postgres.get_endpoint(name=endpoint_path).status.hosts.host
    # user defaults to workspace_client.current_user.me().user_name
    # password = workspace_client.postgres.generate_database_credential(endpoint=endpoint_path).token
    # psycopg.connect(..., sslmode="require"); returns the connection (autocommit False)
```
Use parameterised queries only (no string formatting of values). Use `psycopg.rows.dict_row`.

## Tests
- `connect` with a MagicMock workspace client (check that the args are passed through and the defaults work).
- Real SQL tests against a throwaway local Postgres. A session-scoped fixture runs `initdb` into a tmp dir and
  `pg_ctl -o "-p <free port> -k <tmpdir>" start`, creates a database, yields a connection, and stops it at the end.
  Binaries are at `/opt/homebrew/opt/postgresql@17/bin/` (fall back to `shutil.which("initdb")`). Skip the
  module cleanly if they are not available. Cover: ensure_schema is idempotent; upsert insert plus update
  while PENDING; upsert does NOT overwrite an APPROVED row; each action and its audit row; validation
  errors; missing row returns False; fetch_decisions excludes PENDING; status_summary counts.
