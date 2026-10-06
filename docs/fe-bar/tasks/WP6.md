# WP6: Genie space as code + benchmark

## Goal
A Genie space, "Halvard Group Property KPIs", over the harmonized data, defined as code so it is
reproducible, plus a benchmark script whose text output is committed as execution evidence.

## Reference
`.ref/example-serialized-space.json` is a real `serialized_space` (version 2) export. Read it for the exact
structure: `data_sources.tables[]` (`identifier`), `config.sample_questions[]` (`id`, `question: [str]`),
`instructions.text_instructions[]` (`id`, `content: [str]`), `instructions.example_question_sqls[]`
(`id`, `question: [str]`, `sql: [str]`). IDs are 32-char lowercase hex strings. Do not commit `.ref/`.
Check in the reference whether lists must be sorted (e.g. tables by identifier, items by id); if they are,
sort them.

## Files
- NEW `genie/space_config.yaml`: title, description, warehouse placeholder, tables (relative names, formatted
  with catalog/schema): `harmonized_property_monthly`, `mv_group_property_kpis`, `column_mapping_dictionary`,
  `data_quality_results`, `mapping_eval_results`. Text instructions for a group-finance audience: that loss
  ratio = claims incurred / GWP; that amounts are in EUR; that country means the reporting subsidiary; to prefer the
  metric view for KPI questions; and how mapping provenance is answered from the dictionary. 6 sample
  questions, e.g. "What is the loss ratio by country for the last 3 months?", "Which distribution channel has
  the highest combined ratio in Italy?", "How many columns of the Italian feed were mapped by AI and approved
  without correction?", "Show gross written premium by month and country", "Which data quality checks failed
  in the last publish run?", "Which local column feeds net_written_premium_eur in Spain?". At least 3 example
  SQLs (use `MEASURE()` syntax for metric view queries: `SELECT \`Country\`, MEASURE(\`Loss Ratio\`) FROM mv GROUP BY ALL`).
- NEW `src/harmonization/genie_space.py`: pure. `load_space_config(path)`, `build_serialized_space(cfg, catalog, schema) -> dict`
  (version 2, deterministic IDs derived from a hash of the content, so reruns do not churn), `to_api_payload(cfg, catalog, schema,
  warehouse_id, parent_path) -> dict` (title, description, warehouse_id, parent_path, serialized_space as a JSON string).
- NEW `scripts/genie_space.py`: CLI (`argparse`): `--profile`, `--catalog`, `--schema`, `--warehouse-id`, `--space-id`
  (optional; update when given, else create), `--out` (writes the created space id to a file). Uses
  `WorkspaceClient(profile=...)`. Prefer `w.genie.create_space` / `w.genie.update_space` if they exist in the installed
  SDK; otherwise `w.api_client.do("POST"/"PATCH", "/api/2.0/genie/spaces...")` (state in WP_NOTES which one you used).
  Prints the space id and URL.
- NEW `scripts/genie_benchmark.py`: `--profile`, `--space-id`, `--questions` (default: the sample questions from the
  config), `--out evidence/genie_benchmark.md`. For each question: `w.genie.start_conversation_and_wait(space_id, content)`,
  collects the text answer, generated SQL and query description from attachments, fetches up to 10 result rows
  (`w.genie.get_message_attachment_query_result` or the statement result), and writes markdown: timestamp, space id,
  then per question: status, answer, SQL in a ```sql block, and a markdown table of rows. Ends with a summary
  "answered with SQL: x/y". Robust: a failing question is recorded with its error and the script continues.
  Keep the markdown rendering in a pure function `render_benchmark(results) -> str` in `genie_space.py` so it is tested.
- NEW `tests/test_genie_space.py`

## Tests
Serialized-space structure matches the reference shape; IDs are deterministic and 32-hex; table identifiers are
fully qualified; payload serialized_space is a string that round-trips via json; render_benchmark with success,
no-SQL and error cases.
