from __future__ import annotations

import pytest

from experiment_filters import (
    compile_filters,
    dump_filter_settings,
    filter_row_indexes,
    filter_row_indexes_by_group,
    normalize_filter_logic,
    parse_filter_settings,
    row_matches_filters,
    FilterGroup,
    CompiledFilter,
)


ROWS = [
    {"experiment_id": "run-1", "tag": "baseline", "score": "0.81", "notes": "", "epoch": "10"},
    {"experiment_id": "run-2", "tag": "augmented", "score": "0.93", "notes": "kept", "epoch": "25"},
    {"experiment_id": "run-3", "tag": "baseline-large", "score": "1.20", "epoch": "40"},
]


def test_filter_row_indexes_matches_contains_case_insensitive() -> None:
    indexes = filter_row_indexes(ROWS, [{"column": "tag", "operator": "contains", "value": "BASE"}])
    assert indexes == [0, 2]


def test_filter_row_indexes_matches_any_contains_value_from_list() -> None:
    indexes = filter_row_indexes(ROWS, [{"column": "tag", "value": ["aug", "large"]}])
    assert indexes == [1, 2]


def test_filter_row_indexes_matches_missing_values_and_absent_keys() -> None:
    indexes = filter_row_indexes(ROWS, [{"column": "notes", "operator": "missing"}])
    assert indexes == [0, 2]


def test_filter_row_indexes_matches_numeric_less_than() -> None:
    indexes = filter_row_indexes(ROWS, [{"column": "score", "operator": "lt", "value": 1.0}])
    assert indexes == [0, 1]


def test_filter_row_indexes_matches_numeric_greater_than() -> None:
    indexes = filter_row_indexes(ROWS, [{"column": "epoch", "operator": ">", "value": "20"}])
    assert indexes == [1, 2]


def test_row_matches_filters_requires_all_filters_to_match() -> None:
    compiled = compile_filters(
        [
            {"column": "tag", "value": "baseline"},
            {"column": "score", "operator": "gt", "value": "1.0"},
        ]
    )

    assert row_matches_filters(ROWS[2], compiled) is True
    assert row_matches_filters(ROWS[0], compiled) is False


def test_filter_row_indexes_with_or_logic_matches_any_filter() -> None:
    indexes = filter_row_indexes(
        ROWS,
        [
            {"column": "tag", "operator": "contains", "value": "aug"},
            {"column": "notes", "operator": "missing"},
        ],
        combine_with="or",
    )
    assert indexes == [0, 1, 2]


def test_normalize_filter_logic_rejects_unknown_values() -> None:
    with pytest.raises(ValueError, match="Unsupported filter logic"):
        normalize_filter_logic("xor")


@pytest.mark.parametrize(
    ("filter_spec", "message"),
    [
        ({"column": "tag", "operator": "unknown", "value": "baseline"}, "Unsupported filter operator"),
        ({"column": "score", "operator": "lt", "value": "not-a-number"}, "Numeric filter value is required"),
        ({"column": "tag", "operator": "contains", "value": ""}, "Filter value is required"),
        ({"operator": "contains", "value": "baseline"}, "Filter column is required"),
    ],
)
def test_compile_filters_rejects_invalid_specs(filter_spec: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        compile_filters([filter_spec])


# ---------------------------------------------------------------------------
# not_contains operator
# ---------------------------------------------------------------------------


def test_filter_row_indexes_not_contains_excludes_matching_rows() -> None:
    indexes = filter_row_indexes(ROWS, [{"column": "tag", "operator": "not_contains", "value": "baseline"}])
    assert indexes == [1]


def test_filter_row_indexes_not_contains_alias_does_not_contain() -> None:
    indexes = filter_row_indexes(ROWS, [{"column": "tag", "operator": "does_not_contain", "value": "aug"}])
    assert indexes == [0, 2]


def test_filter_row_indexes_not_contains_with_list_value() -> None:
    # Should exclude rows whose tag contains *any* of the supplied values.
    indexes = filter_row_indexes(ROWS, [{"column": "tag", "operator": "not_contains", "value": ["aug", "large"]}])
    assert indexes == [0]


# ---------------------------------------------------------------------------
# YAML round-trip
# ---------------------------------------------------------------------------


def test_parse_filter_settings_extracts_bucket_prefix_and_filters() -> None:
    yaml_text = (
        'bucket: "mybucket"\n'
        'prefix: "runs"\n'
        "filter_logic: or\n"
        "filters:\n"
        '  - tag: "baseline"\n'
        '  - score: { op: gt, value: "0.9" }\n'
        "  - notes: { op: missing }\n"
        '  - label: { op: not_contains, value: "debug" }\n'
        "matching_experiments:\n"
        '  - "s3://mybucket/runs/exp-1"\n'
    )
    settings = parse_filter_settings(yaml_text)

    assert settings["bucket"] == "mybucket"
    assert settings["prefix"] == "runs"
    assert settings["filter_logic"] == "or"
    assert settings["matching_experiments"] == ["s3://mybucket/runs/exp-1"]

    filters = settings["filters"]
    assert {"column": "tag", "operator": "contains", "value": "baseline"} in filters
    assert {"column": "score", "operator": "gt", "value": "0.9"} in filters
    assert {"column": "notes", "operator": "missing"} in filters
    assert {"column": "label", "operator": "not_contains", "value": "debug"} in filters


def test_parse_filter_settings_drops_unknown_columns_when_known_columns_given() -> None:
    yaml_text = "filters:\n  - tag: \"baseline\"\n  - unknown_col: \"value\"\n"
    settings = parse_filter_settings(yaml_text, known_columns=["tag"])
    columns = [f["column"] for f in settings["filters"]]
    assert columns == ["tag"]


def test_dump_filter_settings_round_trips_correctly() -> None:
    settings = {
        "bucket": "mb",
        "prefix": "p",
        "filter_logic": "or",
        "filters": [
            {"column": "tag", "operator": "contains", "value": "base"},
            {"column": "score", "operator": "lt", "value": "1.0"},
            {"column": "notes", "operator": "missing"},
            {"column": "label", "operator": "not_contains", "value": "debug"},
        ],
        "matching_experiments": ["s3://mb/p/run-1"],
    }
    yaml_text = dump_filter_settings(settings)
    parsed = parse_filter_settings(yaml_text)

    assert parsed["bucket"] == "mb"
    assert parsed["prefix"] == "p"
    assert parsed["filter_logic"] == "or"
    assert parsed["matching_experiments"] == ["s3://mb/p/run-1"]

    filter_map = {f["column"]: f for f in parsed["filters"]}
    assert filter_map["tag"]["operator"] == "contains"
    assert filter_map["score"]["operator"] == "lt"
    assert filter_map["notes"]["operator"] == "missing"
    assert filter_map["label"]["operator"] == "not_contains"


def test_dump_filter_settings_omits_is_one_of_for_single_value() -> None:
    settings = {
        "bucket": "mb",
        "prefix": "p",
        "filters": [
            {"column": "tag", "operator": "is_one_of", "value": ["baseline"]},
        ],
    }
    text = dump_filter_settings(settings)

    assert "is_one_of" not in text
    assert '- tag: "baseline"' in text


def test_dump_filter_settings_produces_empty_list_when_no_filters() -> None:
    settings = {"bucket": "b", "prefix": "", "filters": [], "matching_experiments": []}
    text = dump_filter_settings(settings)
    assert "filters:\n  []" in text
    assert "matching_experiments:\n  []" in text


def test_parse_filter_settings_handles_list_value_shorthand() -> None:
    yaml_text = 'filters:\n  - tag: ["base", "aug"]\n'
    settings = parse_filter_settings(yaml_text)
    assert settings["filters"][0] == {"column": "tag", "operator": "contains", "value": ["base", "aug"]}


# ---------------------------------------------------------------------------
# Hierarchical filter groups (nested AND/OR)
# ---------------------------------------------------------------------------


def test_filter_row_indexes_by_group_simple_and_group() -> None:
    """Test simple AND group: filter1 AND filter2."""
    compiled = compile_filters([
        {"column": "tag", "operator": "contains", "value": "baseline"},
        {"column": "score", "operator": "gt", "value": "0.9"},
    ])
    group = FilterGroup(logic="and", items=tuple(compiled))
    indexes = filter_row_indexes_by_group(ROWS, group)
    assert indexes == [2]  # Only row 2 has "baseline" and score > 0.9


def test_filter_row_indexes_by_group_simple_or_group() -> None:
    """Test simple OR group: filter1 OR filter2."""
    compiled = compile_filters([
        {"column": "tag", "operator": "contains", "value": "aug"},
        {"column": "score", "operator": "lt", "value": "0.5"},
    ])
    group = FilterGroup(logic="or", items=tuple(compiled))
    indexes = filter_row_indexes_by_group(ROWS, group)
    assert indexes == [1]  # Row 1 has "aug"; no rows have score < 0.5


def test_filter_row_indexes_by_group_nested_structure() -> None:
    """Test nested groups: (tag contains baseline AND score > 0.9) OR (epoch > 30)."""
    # Inner AND group: tag contains baseline AND score > 0.9
    and_compiled = compile_filters([
        {"column": "tag", "operator": "contains", "value": "baseline"},
        {"column": "score", "operator": "gt", "value": "0.9"},
    ])
    and_group = FilterGroup(logic="and", items=tuple(and_compiled))
    
    # Outer filter: epoch > 30
    epoch_compiled = compile_filters([{"column": "epoch", "operator": "gt", "value": "30"}])
    
    # Outer OR group: and_group OR epoch_filter
    group = FilterGroup(logic="or", items=(and_group, epoch_compiled[0]))
    indexes = filter_row_indexes_by_group(ROWS, group)
    # Row 2 matches: baseline AND score=1.20 > 0.9 (AND part), and epoch=40 > 30 (OR part)
    # Only row 2 matches the OR condition
    assert sorted(indexes) == [2]


def test_filter_row_indexes_by_group_empty_group() -> None:
    """Test empty filter group returns all rows."""
    group = FilterGroup(logic="and", items=())
    indexes = filter_row_indexes_by_group(ROWS, group)
    assert indexes == [0, 1, 2]


def test_filter_row_indexes_by_group_single_filter() -> None:
    """Test group with single filter."""
    compiled = compile_filters([{"column": "tag", "operator": "contains", "value": "baseline"}])
    group = FilterGroup(logic="and", items=tuple(compiled))
    indexes = filter_row_indexes_by_group(ROWS, group)
    assert indexes == [0, 2]


def test_filter_row_indexes_by_group_deeply_nested() -> None:
    """Test deeply nested structure: ((A AND B) OR C) AND (D OR E)."""
    # A: tag contains baseline
    a = compile_filters([{"column": "tag", "operator": "contains", "value": "baseline"}])[0]
    # B: score > 0.9
    b = compile_filters([{"column": "score", "operator": "gt", "value": "0.9"}])[0]
    # C: notes is missing
    c = compile_filters([{"column": "notes", "operator": "missing"}])[0]
    
    # Inner left: (A AND B)
    inner_left = FilterGroup(logic="and", items=(a, b))
    # Left part: ((A AND B) OR C)
    left_part = FilterGroup(logic="or", items=(inner_left, c))
    
    # D: epoch > 10
    d = compile_filters([{"column": "epoch", "operator": "gt", "value": "10"}])[0]
    # E: score < 0.5
    e = compile_filters([{"column": "score", "operator": "lt", "value": "0.5"}])[0]
    
    # Right part: (D OR E)
    right_part = FilterGroup(logic="or", items=(d, e))
    
    # Outer: ((A AND B) OR C) AND (D OR E)
    group = FilterGroup(logic="and", items=(left_part, right_part))
    indexes = filter_row_indexes_by_group(ROWS, group)
    
    # Left part: ((A AND B) OR C)
    #   - A AND B: rows with tag contains "baseline" AND score > 0.9 = row 2
    #   - C: rows with notes missing = rows 0, 2
    #   - ((A AND B) OR C) = rows 2 OR rows 0,2 = rows 0, 2
    # Right part: (D OR E)
    #   - D (epoch > 10): rows 1, 2
    #   - E (score < 0.5): rows (none)
    #   - (D OR E) = rows 1, 2
    # Outer AND: rows 0,2 AND rows 1,2 = row 2
    assert sorted(indexes) == [2]


def test_filter_group_negate_inverts_result() -> None:
    """A negated group should return the complement of the un-negated group."""
    compiled = compile_filters([{"column": "tag", "operator": "contains", "value": "baseline"}])
    # Without negate: rows 0, 2
    plain_group = FilterGroup(logic="and", items=tuple(compiled), negate=False)
    assert filter_row_indexes_by_group(ROWS, plain_group) == [0, 2]
    # With negate: row 1 (the complement)
    negated_group = FilterGroup(logic="and", items=tuple(compiled), negate=True)
    assert filter_row_indexes_by_group(ROWS, negated_group) == [1]


def test_filter_group_negate_on_nested_group() -> None:
    """Negation applies to the subgroup result in a nested structure."""
    # Baseline rows are 0, 2; NOT baseline rows is 1.
    a = compile_filters([{"column": "tag", "operator": "contains", "value": "baseline"}])[0]
    not_baseline = FilterGroup(logic="and", items=(a,), negate=True)
    # Epoch > 10 rows are 1, 2.
    b = compile_filters([{"column": "epoch", "operator": "gt", "value": "10"}])[0]
    outer = FilterGroup(logic="and", items=(not_baseline, b))
    # NOT(baseline) AND epoch>10 = {1} AND {1,2} = row 1
    assert filter_row_indexes_by_group(ROWS, outer) == [1]
