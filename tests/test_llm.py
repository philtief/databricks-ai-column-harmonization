"""Tests for harmonization.llm — the parameterizable LLM mapping module."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from harmonization.llm import (
    DEFAULT_CONFIDENCE_LEVELS,
    DEFAULT_MATCH_TYPES,
    DEFAULT_PROMPT_TEMPLATE,
    DEFAULT_RESPONSE_KEYS,
    LLMConfig,
    _sql_quote,
    build_ai_query_sql,
    build_mapping_prompt,
    estimate_cost,
    load_llm_config,
    parse_mapping_response,
    render_response_schema,
    render_static_prompt,
    template_to_sql_concat,
)

# ----------------------------- LLMConfig defaults -----------------------------


class TestLLMConfigDefaults:
    def test_defaults_are_set(self):
        cfg = LLMConfig()
        assert cfg.endpoint == "databricks-gpt-5-2"
        assert cfg.prompt_template == DEFAULT_PROMPT_TEMPLATE
        assert cfg.match_types == DEFAULT_MATCH_TYPES
        assert cfg.confidence_levels == DEFAULT_CONFIDENCE_LEVELS
        assert cfg.response_keys == DEFAULT_RESPONSE_KEYS
        assert cfg.estimated_prompt_tokens_per_column == 200.0
        assert cfg.estimated_response_tokens_per_column == 80.0
        assert cfg.estimated_cost_per_1k_tokens == 0.002
        assert cfg.extra_context == {}

    def test_is_frozen(self):
        from dataclasses import FrozenInstanceError

        cfg = LLMConfig()
        with pytest.raises(FrozenInstanceError):
            cfg.endpoint = "other"  # type: ignore[misc]

    def test_replace_creates_new_instance(self):
        cfg = LLMConfig()
        new_cfg = replace(cfg, endpoint="my-endpoint")
        assert new_cfg.endpoint == "my-endpoint"
        assert cfg.endpoint == "databricks-gpt-5-2"


# ----------------------------- load_llm_config --------------------------------


class TestLoadLLMConfig:
    def test_no_ai_block_returns_defaults(self):
        cfg = load_llm_config({})
        assert cfg == LLMConfig()

    def test_empty_ai_block_returns_defaults(self):
        cfg = load_llm_config({"ai": {}})
        assert cfg == LLMConfig()

    def test_endpoint_from_yaml(self):
        cfg = load_llm_config({"ai": {"endpoint": "claude-3"}})
        assert cfg.endpoint == "claude-3"

    def test_endpoint_legacy_default_endpoint_key(self):
        cfg = load_llm_config({"ai": {"default_endpoint": "legacy"}})
        assert cfg.endpoint == "legacy"

    def test_endpoint_takes_priority_over_default_endpoint(self):
        cfg = load_llm_config({"ai": {"endpoint": "primary", "default_endpoint": "legacy"}})
        assert cfg.endpoint == "primary"

    def test_custom_prompt_template(self):
        tpl = "${context} - ${target_columns} - ${local_column_name}"
        cfg = load_llm_config({"ai": {"prompt_template": tpl}})
        assert cfg.prompt_template == tpl

    def test_blank_prompt_template_keeps_default(self):
        cfg = load_llm_config({"ai": {"prompt_template": ""}})
        assert cfg.prompt_template == DEFAULT_PROMPT_TEMPLATE

    def test_custom_vocabularies(self):
        cfg = load_llm_config(
            {
                "ai": {
                    "match_types": ["EXACT", "FUZZY", "NONE"],
                    "confidence_levels": ["A", "B", "C", "D"],
                    "response_keys": ["target", "type", "reason", "score"],
                }
            }
        )
        assert cfg.match_types == ("EXACT", "FUZZY", "NONE")
        assert cfg.confidence_levels == ("A", "B", "C", "D")
        assert cfg.response_keys == ("target", "type", "reason", "score")

    def test_cost_estimates(self):
        cfg = load_llm_config(
            {
                "ai": {
                    "estimated_prompt_tokens_per_column": 500,
                    "estimated_response_tokens_per_column": 100,
                    "estimated_cost_per_1k_tokens": 0.01,
                }
            }
        )
        assert cfg.estimated_prompt_tokens_per_column == 500.0
        assert cfg.estimated_response_tokens_per_column == 100.0
        assert cfg.estimated_cost_per_1k_tokens == 0.01

    def test_legacy_estimated_eur_per_1k_tokens_key(self):
        cfg = load_llm_config({"ai": {"estimated_eur_per_1k_tokens": 0.005}})
        assert cfg.estimated_cost_per_1k_tokens == 0.005

    def test_estimated_cost_takes_priority_over_legacy(self):
        cfg = load_llm_config(
            {
                "ai": {
                    "estimated_cost_per_1k_tokens": 0.01,
                    "estimated_eur_per_1k_tokens": 0.005,
                }
            }
        )
        assert cfg.estimated_cost_per_1k_tokens == 0.01

    def test_extra_context_is_passed_through(self):
        cfg = load_llm_config({"ai": {"extra_context": {"language": "German", "tone": "formal"}}})
        assert cfg.extra_context == {"language": "German", "tone": "formal"}


# ----------------------------- render_response_schema -------------------------


class TestRenderResponseSchema:
    def test_default_keys(self):
        schema = render_response_schema(DEFAULT_RESPONSE_KEYS, DEFAULT_MATCH_TYPES)
        decoded = json.loads(schema)
        assert decoded == {
            "global_column_name": "...",
            "match_type": "DIRECT|SEMANTIC_TRANSLATION|DERIVED|NO_MATCH",
            "rationale": "...",
            "confidence": "HIGH|MEDIUM|LOW",
        }

    def test_custom_match_types_in_schema(self):
        schema = render_response_schema(("global_column_name", "match_type"), ("EXACT", "FUZZY"))
        decoded = json.loads(schema)
        assert decoded["match_type"] == "EXACT|FUZZY"

    def test_custom_response_keys_get_ellipsis(self):
        schema = render_response_schema(("target", "score"), DEFAULT_MATCH_TYPES)
        decoded = json.loads(schema)
        assert decoded == {"target": "...", "score": "..."}

    def test_compact_json_no_spaces(self):
        schema = render_response_schema(DEFAULT_RESPONSE_KEYS, DEFAULT_MATCH_TYPES)
        assert ", " not in schema
        assert ": " not in schema


# ----------------------------- render_static_prompt ---------------------------


class TestRenderStaticPrompt:
    def test_substitutes_static_placeholders(self):
        out = render_static_prompt(
            DEFAULT_PROMPT_TEMPLATE,
            ai_context="Some context.",
            target_columns=["a", "b"],
            match_types=DEFAULT_MATCH_TYPES,
            confidence_levels=DEFAULT_CONFIDENCE_LEVELS,
            response_keys=DEFAULT_RESPONSE_KEYS,
        )
        assert "Some context." in out
        assert "a, b, NO_MATCH" in out
        assert "HIGH|MEDIUM|LOW" in out or "global_column_name" in out

    def test_keeps_dynamic_placeholders_unfilled(self):
        out = render_static_prompt(
            DEFAULT_PROMPT_TEMPLATE,
            ai_context="ctx",
            target_columns=["a"],
            match_types=DEFAULT_MATCH_TYPES,
            confidence_levels=DEFAULT_CONFIDENCE_LEVELS,
            response_keys=DEFAULT_RESPONSE_KEYS,
        )
        assert "${local_column_name}" in out
        assert "${local_data_type}" in out
        assert "${sample_values}" in out

    def test_target_with_no_match_appended(self):
        out = render_static_prompt(
            "${target_columns}",
            ai_context="",
            target_columns=["x", "y"],
            match_types=DEFAULT_MATCH_TYPES,
            confidence_levels=DEFAULT_CONFIDENCE_LEVELS,
            response_keys=DEFAULT_RESPONSE_KEYS,
        )
        assert out == "x, y, NO_MATCH"

    def test_target_no_match_already_present_not_duplicated(self):
        out = render_static_prompt(
            "${target_columns}",
            ai_context="",
            target_columns=["x", "NO_MATCH"],
            match_types=DEFAULT_MATCH_TYPES,
            confidence_levels=DEFAULT_CONFIDENCE_LEVELS,
            response_keys=DEFAULT_RESPONSE_KEYS,
        )
        assert out == "x, NO_MATCH"

    def test_target_with_no_match_disabled(self):
        out = render_static_prompt(
            "${target_columns}",
            ai_context="",
            target_columns=["x", "y"],
            match_types=DEFAULT_MATCH_TYPES,
            confidence_levels=DEFAULT_CONFIDENCE_LEVELS,
            response_keys=DEFAULT_RESPONSE_KEYS,
            target_with_no_match=False,
        )
        assert out == "x, y"

    def test_extra_context_substitutes(self):
        out = render_static_prompt(
            "Lang=${language} Cols=${target_columns}",
            ai_context="",
            target_columns=["a"],
            match_types=DEFAULT_MATCH_TYPES,
            confidence_levels=DEFAULT_CONFIDENCE_LEVELS,
            response_keys=DEFAULT_RESPONSE_KEYS,
            extra_context={"language": "German"},
        )
        assert out == "Lang=German Cols=a, NO_MATCH"

    def test_extra_context_shadowing_built_in_raises(self):
        with pytest.raises(ValueError, match="shadows a built-in placeholder"):
            render_static_prompt(
                "${context}",
                ai_context="",
                target_columns=["a"],
                match_types=DEFAULT_MATCH_TYPES,
                confidence_levels=DEFAULT_CONFIDENCE_LEVELS,
                response_keys=DEFAULT_RESPONSE_KEYS,
                extra_context={"context": "no"},
            )

    def test_unknown_placeholder_raises(self):
        with pytest.raises(ValueError, match="Unknown placeholder"):
            render_static_prompt(
                "Hello ${nonexistent}",
                ai_context="",
                target_columns=["a"],
                match_types=DEFAULT_MATCH_TYPES,
                confidence_levels=DEFAULT_CONFIDENCE_LEVELS,
                response_keys=DEFAULT_RESPONSE_KEYS,
            )

    def test_strips_ai_context_whitespace(self):
        out = render_static_prompt(
            "${context}",
            ai_context="  hello world  \n",
            target_columns=["a"],
            match_types=DEFAULT_MATCH_TYPES,
            confidence_levels=DEFAULT_CONFIDENCE_LEVELS,
            response_keys=DEFAULT_RESPONSE_KEYS,
        )
        assert out == "hello world"


# ----------------------------- build_mapping_prompt ---------------------------


class TestBuildMappingPrompt:
    def test_full_substitution(self):
        prompt = build_mapping_prompt(
            local_column_name="numero_poliza",
            local_data_type="STRING",
            sample_values=["P-001", "P-002"],
            target_columns=["policy_id", "premium"],
            ai_context="Spanish insurance.",
        )
        assert "numero_poliza" in prompt
        assert "STRING" in prompt
        assert "P-001; P-002" in prompt
        assert "Spanish insurance." in prompt
        assert "policy_id, premium, NO_MATCH" in prompt
        assert "${" not in prompt

    def test_uses_supplied_llm_config(self):
        cfg = LLMConfig(prompt_template="Col=${local_column_name} Targets=${target_columns}")
        prompt = build_mapping_prompt(
            local_column_name="x",
            local_data_type="INT",
            sample_values=[],
            target_columns=["t"],
            ai_context="",
            llm_config=cfg,
        )
        assert prompt == "Col=x Targets=t, NO_MATCH"

    def test_empty_sample_values(self):
        prompt = build_mapping_prompt(
            local_column_name="x",
            local_data_type="STRING",
            sample_values=[],
            target_columns=["a"],
            ai_context="ctx",
        )
        # Should still substitute sample_values to empty string
        assert "${sample_values}" not in prompt


# ----------------------------- _sql_quote -------------------------------------


class TestSqlQuote:
    def test_simple_string(self):
        assert _sql_quote("hello") == "'hello'"

    def test_escapes_single_quote(self):
        assert _sql_quote("it's") == "'it''s'"

    def test_double_single_quote(self):
        assert _sql_quote("a'b'c") == "'a''b''c'"

    def test_empty_string(self):
        assert _sql_quote("") == "''"


# ----------------------------- template_to_sql_concat -------------------------


class TestTemplateToSqlConcat:
    def test_no_placeholders_returns_quoted_literal(self):
        assert template_to_sql_concat("hello") == "'hello'"

    def test_only_dynamic_placeholder(self):
        assert template_to_sql_concat("${local_column_name}") == "local_column_name"

    def test_sample_values_uses_array_join(self):
        assert template_to_sql_concat("${sample_values}") == "array_join(sample_values, '; ')"

    def test_mixed_literal_and_placeholder(self):
        sql = template_to_sql_concat("Name: ${local_column_name}.")
        assert sql == "CONCAT('Name: ', local_column_name, '.')"

    def test_multiple_dynamic_placeholders(self):
        sql = template_to_sql_concat("Col=${local_column_name} Type=${local_data_type} Vals=${sample_values}")
        assert "local_column_name" in sql
        assert "local_data_type" in sql
        assert "array_join(sample_values, '; ')" in sql
        assert sql.startswith("CONCAT(")

    def test_unfilled_static_placeholder_raises(self):
        with pytest.raises(ValueError, match="Unfilled non-dynamic placeholder"):
            template_to_sql_concat("Hello ${context}!")

    def test_escapes_quote_in_literal(self):
        sql = template_to_sql_concat("it's: ${local_column_name}")
        assert "'it''s: '" in sql

    def test_empty_string(self):
        assert template_to_sql_concat("") == "''"


# ----------------------------- build_ai_query_sql -----------------------------


class TestBuildAIQuerySQL:
    def test_basic_structure(self):
        sql = build_ai_query_sql(
            source_view="my_view",
            llm_config=LLMConfig(),
            ai_context="ctx",
            target_columns=["a", "b"],
        )
        assert sql.startswith("SELECT\n")
        assert "FROM my_view" in sql
        assert "ai_query('databricks-gpt-5-2'" in sql
        assert "AS ai_result" in sql

    def test_endpoint_is_quoted(self):
        sql = build_ai_query_sql(
            source_view="v",
            llm_config=LLMConfig(endpoint="my-endpoint"),
            ai_context="",
            target_columns=["a"],
        )
        assert "ai_query('my-endpoint'" in sql

    def test_endpoint_with_quote_is_escaped(self):
        sql = build_ai_query_sql(
            source_view="v",
            llm_config=LLMConfig(endpoint="end'point"),
            ai_context="",
            target_columns=["a"],
        )
        assert "ai_query('end''point'" in sql

    def test_target_columns_in_sql(self):
        sql = build_ai_query_sql(
            source_view="v",
            llm_config=LLMConfig(),
            ai_context="",
            target_columns=["policy_id", "premium"],
        )
        assert "policy_id, premium, NO_MATCH" in sql

    def test_dynamic_columns_referenced(self):
        sql = build_ai_query_sql(
            source_view="v",
            llm_config=LLMConfig(),
            ai_context="",
            target_columns=["a"],
        )
        assert "local_column_name" in sql
        assert "local_data_type" in sql
        assert "array_join(sample_values, '; ')" in sql

    def test_custom_template_and_vocabularies(self):
        cfg = LLMConfig(
            endpoint="claude",
            prompt_template="Match ${local_column_name} to ${target_columns}. Types: ${match_types}.",
            match_types=("EXACT", "FUZZY"),
        )
        sql = build_ai_query_sql(
            source_view="v",
            llm_config=cfg,
            ai_context="",
            target_columns=["x"],
        )
        assert "Match '" in sql or "'Match '" in sql
        assert "Types: EXACT, FUZZY." in sql
        assert "local_column_name" in sql

    def test_select_columns_present(self):
        sql = build_ai_query_sql(
            source_view="v",
            llm_config=LLMConfig(),
            ai_context="",
            target_columns=["a"],
        )
        for col in ("source_system", "source_table", "local_column_name", "local_data_type", "sample_values"):
            assert col in sql


# ----------------------------- parse_mapping_response -------------------------


class TestParseMappingResponse:
    def test_valid_full_response(self):
        raw = json.dumps(
            {
                "global_column_name": "policy_id",
                "match_type": "DIRECT",
                "rationale": "exact match",
                "confidence": "HIGH",
            }
        )
        out = parse_mapping_response(raw)
        assert out["global_column_name"] == "policy_id"
        assert out["match_type"] == "DIRECT"
        assert out["rationale"] == "exact match"
        assert out["confidence"] == "HIGH"
        assert out["ai_error_status"] is None

    def test_none_input_marks_error(self):
        out = parse_mapping_response(None)
        assert out["ai_error_status"] == "AI_ERROR"
        assert out["global_column_name"] is None

    def test_empty_string_marks_error(self):
        out = parse_mapping_response("")
        assert out["ai_error_status"] == "AI_ERROR"

    def test_whitespace_only_marks_error(self):
        out = parse_mapping_response("   \n  ")
        assert out["ai_error_status"] == "AI_ERROR"

    def test_malformed_json_marks_error(self):
        out = parse_mapping_response("{not valid json")
        assert out["ai_error_status"] == "AI_ERROR"

    def test_json_array_marks_error(self):
        out = parse_mapping_response('["a","b"]')
        assert out["ai_error_status"] == "AI_ERROR"

    def test_json_string_literal_marks_error(self):
        out = parse_mapping_response('"just a string"')
        assert out["ai_error_status"] == "AI_ERROR"

    def test_missing_global_column_name_marks_error(self):
        raw = json.dumps({"match_type": "DIRECT", "confidence": "HIGH"})
        out = parse_mapping_response(raw)
        assert out["ai_error_status"] == "AI_ERROR"
        assert out["global_column_name"] is None

    def test_null_global_column_name_marks_error(self):
        raw = json.dumps({"global_column_name": None, "match_type": "DIRECT"})
        out = parse_mapping_response(raw)
        assert out["ai_error_status"] == "AI_ERROR"

    def test_empty_global_column_name_marks_error(self):
        raw = json.dumps({"global_column_name": "", "match_type": "DIRECT"})
        out = parse_mapping_response(raw)
        assert out["ai_error_status"] == "AI_ERROR"

    def test_partial_response_keeps_present_fields(self):
        raw = json.dumps({"global_column_name": "x", "match_type": "DIRECT"})
        out = parse_mapping_response(raw)
        assert out["global_column_name"] == "x"
        assert out["match_type"] == "DIRECT"
        assert out["rationale"] is None
        assert out["confidence"] is None
        assert out["ai_error_status"] is None

    def test_custom_response_keys(self):
        raw = json.dumps({"target": "x", "reason": "match"})
        out = parse_mapping_response(raw, response_keys=("target", "reason"))
        # 'global_column_name' missing -> error; the function uses
        # response_keys for parsing but always checks global_column_name for validity.
        assert out["target"] == "x"
        assert out["reason"] == "match"

    def test_int_values_coerced_to_str(self):
        raw = json.dumps({"global_column_name": "x", "confidence": 1})
        out = parse_mapping_response(raw)
        assert out["confidence"] == "1"


# ----------------------------- estimate_cost ----------------------------------


class TestEstimateCost:
    def test_zero_columns_zero_cost(self):
        out = estimate_cost(0, LLMConfig())
        assert out == {"prompt_tokens": 0.0, "response_tokens": 0.0, "cost": 0.0}

    def test_default_cost(self):
        out = estimate_cost(100, LLMConfig())
        # 100 cols * 200 prompt + 100 * 80 response = 28000 tokens; * 0.002 / 1000
        assert out["prompt_tokens"] == 20000.0
        assert out["response_tokens"] == 8000.0
        assert out["cost"] == pytest.approx(0.056)

    def test_custom_per_column_estimates(self):
        cfg = LLMConfig(
            estimated_prompt_tokens_per_column=500,
            estimated_response_tokens_per_column=100,
            estimated_cost_per_1k_tokens=0.01,
        )
        out = estimate_cost(10, cfg)
        assert out["prompt_tokens"] == 5000.0
        assert out["response_tokens"] == 1000.0
        assert out["cost"] == pytest.approx(0.06)


# ----------------------------- end-to-end integration -------------------------


class TestEndToEndIntegration:
    """Make sure the pieces compose: YAML -> LLMConfig -> SQL."""

    def test_yaml_to_sql_default_template(self):
        yaml_cfg = {
            "ai": {
                "endpoint": "test-endpoint",
                "estimated_prompt_tokens_per_column": 250,
            }
        }
        llm_cfg = load_llm_config(yaml_cfg)
        assert llm_cfg.endpoint == "test-endpoint"
        sql = build_ai_query_sql(
            source_view="src",
            llm_config=llm_cfg,
            ai_context="domain context",
            target_columns=["c1", "c2"],
        )
        assert "ai_query('test-endpoint'" in sql
        assert "domain context" in sql
        assert "c1, c2, NO_MATCH" in sql
        assert "FROM src" in sql

    def test_custom_template_round_trip(self):
        yaml_cfg = {
            "ai": {
                "endpoint": "ep",
                "prompt_template": (
                    "Source: ${local_column_name}, Type: ${local_data_type}, "
                    "Samples: ${sample_values}, Targets: ${target_columns}, "
                    "Schema: ${response_schema}"
                ),
            }
        }
        llm_cfg = load_llm_config(yaml_cfg)
        sql = build_ai_query_sql(
            source_view="v",
            llm_config=llm_cfg,
            ai_context="",
            target_columns=["a"],
        )
        # Must reference all dynamic columns
        assert "local_column_name" in sql
        assert "local_data_type" in sql
        assert "array_join(sample_values, '; ')" in sql
        # And the static parts must be quoted literals
        assert "'Source: '" in sql
        assert "'Targets: a, NO_MATCH" in sql or "Targets: a, NO_MATCH" in sql

    def test_full_loop_prompt_then_parse(self):
        cfg = LLMConfig()
        prompt = build_mapping_prompt(
            local_column_name="numero_poliza",
            local_data_type="STRING",
            sample_values=["P1", "P2"],
            target_columns=["policy_id"],
            ai_context="ctx",
            llm_config=cfg,
        )
        # Prompt should be fully resolved (no placeholders left)
        assert "${" not in prompt

        # Simulate the LLM responding correctly
        response = json.dumps(
            {
                "global_column_name": "policy_id",
                "match_type": "DIRECT",
                "rationale": "Spanish for policy number",
                "confidence": "HIGH",
            }
        )
        parsed = parse_mapping_response(response, cfg.response_keys)
        assert parsed["global_column_name"] == "policy_id"
        assert parsed["ai_error_status"] is None
