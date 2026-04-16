from __future__ import annotations

import pytest

from experiment_filters import (
    compile_filters,
    dump_filter_settings,
    filter_row_indexes,
    parse_filter_settings,
    row_matches_filters,
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
    assert parsed["matching_experiments"] == ["s3://mb/p/run-1"]

    filter_map = {f["column"]: f for f in parsed["filters"]}
    assert filter_map["tag"]["operator"] == "contains"
    assert filter_map["score"]["operator"] == "lt"
    assert filter_map["notes"]["operator"] == "missing"
    assert filter_map["label"]["operator"] == "not_contains"


def test_dump_filter_settings_produces_empty_list_when_no_filters() -> None:
    settings = {"bucket": "b", "prefix": "", "filters": [], "matching_experiments": []}
    text = dump_filter_settings(settings)
    assert "filters:\n  []" in text
    assert "matching_experiments:\n  []" in text


def test_parse_filter_settings_handles_list_value_shorthand() -> None:
    yaml_text = 'filters:\n  - tag: ["base", "aug"]\n'
    settings = parse_filter_settings(yaml_text)
    assert settings["filters"][0] == {"column": "tag", "operator": "contains", "value": ["base", "aug"]}
