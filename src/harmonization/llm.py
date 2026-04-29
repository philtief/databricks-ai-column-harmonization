"""Configurable LLM mapping logic for column harmonization.

This module is the only place where prompt construction lives. The notebook
calls these helpers to build a vectorized ``ai_query()`` SQL statement; the
prompt template, endpoint, response schema, and labels are all driven by
the user's ``config/harmonization_config.yaml``.

Placeholders use ``${name}`` syntax (``string.Template``) to avoid clashing
with the JSON braces in the response example.

Static placeholders — resolved at Python time, before the SQL is built:
    ${context}            Source-domain description from source_context.description
    ${target_columns}     Comma-joined list of valid target column names + NO_MATCH
    ${match_types}        Comma-joined match-type vocabulary
    ${confidence_levels}  Pipe-joined confidence vocabulary (e.g. HIGH|MEDIUM|LOW)
    ${response_schema}    JSON example showing the keys the model must return

Dynamic placeholders — turned into SQL column references at execution time:
    ${local_column_name}  source column name (string column)
    ${local_data_type}    source column data type (string column)
    ${sample_values}      array<string> column, joined with '; ' inside SQL
"""

from __future__ import annotations

import json
import re
import string
from dataclasses import dataclass, field
from typing import Any

DEFAULT_MATCH_TYPES = ("DIRECT", "SEMANTIC_TRANSLATION", "DERIVED", "NO_MATCH")
DEFAULT_CONFIDENCE_LEVELS = ("HIGH", "MEDIUM", "LOW")
DEFAULT_RESPONSE_KEYS = ("global_column_name", "match_type", "rationale", "confidence")

DEFAULT_PROMPT_TEMPLATE = (
    "You are a data harmonization expert. "
    "Map this source column to the best matching target column from the global model. "
    'Source column name: "${local_column_name}". '
    "Data type: ${local_data_type}. "
    "Sample values: ${sample_values}. "
    "Context: ${context} "
    "Valid target columns: ${target_columns}. "
    "Rules: prefer DIRECT for exact or near-exact matches, "
    "SEMANTIC_TRANSLATION for conceptual equivalents, "
    "DERIVED if the source column is computed from other fields, "
    "NO_MATCH if no safe mapping exists "
    "(e.g. technical metadata columns). "
    "Return ONLY a JSON object with no additional text: ${response_schema}"
)

_DYNAMIC_PLACEHOLDERS = {"local_column_name", "local_data_type", "sample_values"}

_PLACEHOLDER_RE = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


@dataclass(frozen=True)
class LLMConfig:
    """All knobs for the column-mapping LLM call.

    Loaded from the ``ai:`` block of ``harmonization_config.yaml`` via
    :func:`load_llm_config`. All fields are optional in YAML and fall back
    to documented defaults.
    """

    endpoint: str = "databricks-gpt-5-2"
    prompt_template: str = DEFAULT_PROMPT_TEMPLATE
    match_types: tuple[str, ...] = DEFAULT_MATCH_TYPES
    confidence_levels: tuple[str, ...] = DEFAULT_CONFIDENCE_LEVELS
    response_keys: tuple[str, ...] = DEFAULT_RESPONSE_KEYS
    estimated_prompt_tokens_per_column: float = 200.0
    estimated_response_tokens_per_column: float = 80.0
    estimated_cost_per_1k_tokens: float = 0.002
    extra_context: dict[str, str] = field(default_factory=dict)


def load_llm_config(config: dict[str, Any]) -> LLMConfig:
    """Build an :class:`LLMConfig` from a parsed YAML config dict."""
    ai_block = config.get("ai") or {}
    overrides: dict[str, Any] = {}

    endpoint = ai_block.get("endpoint") or ai_block.get("default_endpoint")
    if endpoint:
        overrides["endpoint"] = endpoint

    if ai_block.get("prompt_template"):
        overrides["prompt_template"] = ai_block["prompt_template"]

    if ai_block.get("match_types"):
        overrides["match_types"] = tuple(ai_block["match_types"])

    if ai_block.get("confidence_levels"):
        overrides["confidence_levels"] = tuple(ai_block["confidence_levels"])

    if ai_block.get("response_keys"):
        overrides["response_keys"] = tuple(ai_block["response_keys"])

    for cost_key in ("estimated_prompt_tokens_per_column", "estimated_response_tokens_per_column"):
        if cost_key in ai_block:
            overrides[cost_key] = float(ai_block[cost_key])

    cost_per_1k = ai_block.get("estimated_cost_per_1k_tokens")
    if cost_per_1k is None:
        cost_per_1k = ai_block.get("estimated_eur_per_1k_tokens")
    if cost_per_1k is not None:
        overrides["estimated_cost_per_1k_tokens"] = float(cost_per_1k)

    if ai_block.get("extra_context"):
        overrides["extra_context"] = dict(ai_block["extra_context"])

    return LLMConfig(**overrides)


def render_response_schema(response_keys: tuple[str, ...], match_types: tuple[str, ...]) -> str:
    """Render an example JSON object the model is asked to return."""
    example: dict[str, str] = {}
    for key in response_keys:
        if key == "global_column_name":
            example[key] = "..."
        elif key == "match_type":
            example[key] = "|".join(match_types)
        elif key == "confidence":
            example[key] = "HIGH|MEDIUM|LOW"
        else:
            example[key] = "..."
    return json.dumps(example, separators=(",", ":"))


def render_static_prompt(
    template: str,
    *,
    ai_context: str,
    target_columns: list[str],
    match_types: tuple[str, ...],
    confidence_levels: tuple[str, ...],
    response_keys: tuple[str, ...],
    extra_context: dict[str, str] | None = None,
    target_with_no_match: bool = True,
) -> str:
    """Substitute the static placeholders in the template.

    Dynamic placeholders (``${local_column_name}`` etc.) are left as-is so
    they can be turned into SQL column references later.
    """
    targets = list(target_columns)
    if target_with_no_match and "NO_MATCH" not in targets:
        targets.append("NO_MATCH")

    static_vars: dict[str, str] = {
        "context": ai_context.strip(),
        "target_columns": ", ".join(targets),
        "match_types": ", ".join(match_types),
        "confidence_levels": "|".join(confidence_levels),
        "response_schema": render_response_schema(response_keys, match_types),
    }
    if extra_context:
        for k, v in extra_context.items():
            if k in static_vars:
                raise ValueError(f"extra_context key '{k}' shadows a built-in placeholder")
            static_vars[k] = str(v)

    # Validate template references only known placeholders
    for placeholder in _PLACEHOLDER_RE.findall(template):
        if placeholder not in _DYNAMIC_PLACEHOLDERS and placeholder not in static_vars:
            raise ValueError(
                f"Unknown placeholder '${{{placeholder}}}' in prompt template. "
                f"Known dynamic placeholders: {sorted(_DYNAMIC_PLACEHOLDERS)}. "
                f"Known static placeholders: {sorted(static_vars.keys())}."
            )

    # We use a custom Template subclass so that {dynamic} placeholders aren't
    # treated as missing — we want to leave them in place for SQL substitution.
    class _PartialTemplate(string.Template):
        idpattern = "|".join(re.escape(k) for k in static_vars)

    return _PartialTemplate(template).safe_substitute(static_vars)


def build_mapping_prompt(
    *,
    local_column_name: str,
    local_data_type: str,
    sample_values: list[str],
    target_columns: list[str],
    ai_context: str,
    llm_config: LLMConfig | None = None,
) -> str:
    """Render the full prompt for a single column.

    Useful for tests and for invoking the LLM outside the vectorized SQL path
    (e.g. for ad-hoc experimentation).
    """
    cfg = llm_config or LLMConfig()
    static_filled = render_static_prompt(
        cfg.prompt_template,
        ai_context=ai_context,
        target_columns=target_columns,
        match_types=cfg.match_types,
        confidence_levels=cfg.confidence_levels,
        response_keys=cfg.response_keys,
        extra_context=cfg.extra_context,
    )
    return string.Template(static_filled).safe_substitute(
        local_column_name=local_column_name,
        local_data_type=local_data_type,
        sample_values="; ".join(sample_values),
    )


def _sql_quote(literal: str) -> str:
    """Wrap a string literal for inclusion in a SQL CONCAT()."""
    return "'" + literal.replace("'", "''") + "'"


def template_to_sql_concat(static_filled_template: str) -> str:
    """Convert a prompt template (static placeholders already filled) into a
    Spark SQL ``CONCAT(...)`` expression with the dynamic placeholders turned
    into column references.

    The dynamic placeholders use ``sample_values`` (an ARRAY<STRING> column)
    via ``array_join(sample_values, '; ')`` so the model sees a flat string.
    """
    parts: list[str] = []
    last = 0
    for match in _PLACEHOLDER_RE.finditer(static_filled_template):
        name = match.group(1)
        if name not in _DYNAMIC_PLACEHOLDERS:
            raise ValueError(
                f"Unfilled non-dynamic placeholder '${{{name}}}' encountered in template. "
                "Did you forget to call render_static_prompt() first?"
            )
        if match.start() > last:
            parts.append(_sql_quote(static_filled_template[last : match.start()]))
        if name == "sample_values":
            parts.append("array_join(sample_values, '; ')")
        else:
            parts.append(name)
        last = match.end()
    if last < len(static_filled_template):
        parts.append(_sql_quote(static_filled_template[last:]))
    if not parts:
        parts.append("''")
    if len(parts) == 1:
        return parts[0]
    return "CONCAT(" + ", ".join(parts) + ")"


def build_ai_query_sql(
    *,
    source_view: str,
    llm_config: LLMConfig,
    ai_context: str,
    target_columns: list[str],
) -> str:
    """Build the full vectorized ``ai_query`` SQL statement.

    The result is a SELECT that returns one row per source column with an
    ``ai_result`` JSON string from the LLM, ready to be parsed by Spark
    ``from_json`` in the calling notebook.
    """
    static_filled = render_static_prompt(
        llm_config.prompt_template,
        ai_context=ai_context,
        target_columns=target_columns,
        match_types=llm_config.match_types,
        confidence_levels=llm_config.confidence_levels,
        response_keys=llm_config.response_keys,
        extra_context=llm_config.extra_context,
    )
    prompt_concat = template_to_sql_concat(static_filled)
    endpoint_sql = _sql_quote(llm_config.endpoint)

    return (
        "SELECT\n"
        "  source_system,\n"
        "  source_table,\n"
        "  local_column_name,\n"
        "  local_data_type,\n"
        "  sample_values,\n"
        "  array_join(sample_values, '; ') AS sample_str,\n"
        f"  ai_query({endpoint_sql}, {prompt_concat}) AS ai_result\n"
        f"FROM {source_view}"
    )


def parse_mapping_response(
    raw: str | None,
    response_keys: tuple[str, ...] = DEFAULT_RESPONSE_KEYS,
) -> dict[str, str | None]:
    """Parse a JSON LLM response into a dict keyed by ``response_keys``.

    Returns a dict with the requested keys (set to ``None`` when missing) and
    a synthetic ``ai_error_status`` key set to ``"AI_ERROR"`` if the response
    is unparseable, empty, or missing the primary ``global_column_name``.
    """
    out: dict[str, str | None] = {k: None for k in response_keys}
    out["ai_error_status"] = None

    if raw is None or not str(raw).strip():
        out["ai_error_status"] = "AI_ERROR"
        return out

    try:
        decoded = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        out["ai_error_status"] = "AI_ERROR"
        return out

    if not isinstance(decoded, dict):
        out["ai_error_status"] = "AI_ERROR"
        return out

    for key in response_keys:
        val = decoded.get(key)
        out[key] = val if val is None else str(val)

    if not out.get("global_column_name"):
        out["ai_error_status"] = "AI_ERROR"

    return out


def estimate_cost(n_columns: int, llm_config: LLMConfig) -> dict[str, float]:
    """Return estimated prompt/response tokens and dollar/euro cost.

    The unit of currency is whatever the user expressed
    ``estimated_cost_per_1k_tokens`` in (no opinion is taken).
    """
    prompt = n_columns * llm_config.estimated_prompt_tokens_per_column
    response = n_columns * llm_config.estimated_response_tokens_per_column
    cost = ((prompt + response) / 1000.0) * llm_config.estimated_cost_per_1k_tokens
    return {"prompt_tokens": prompt, "response_tokens": response, "cost": cost}
