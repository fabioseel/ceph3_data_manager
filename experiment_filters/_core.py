from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence, Union


_OPERATOR_ALIASES: dict[str, str] = {
    "contains": "contains",
    "match": "contains",
    "matches": "contains",
    "not_contains": "not_contains",
    "does_not_contain": "not_contains",
    "not_match": "not_contains",
    "not_matches": "not_contains",
    "!contains": "not_contains",
    "is_one_of": "is_one_of",
    "is one of": "is_one_of",
    "in": "is_one_of",
    "is_not_one_of": "is_not_one_of",
    "is not one of": "is_not_one_of",
    "not_in": "is_not_one_of",
    "missing": "missing",
    "absent": "missing",
    "is_missing": "missing",
    "lt": "lt",
    "less": "lt",
    "less_than": "lt",
    "<": "lt",
    "gt": "gt",
    "greater": "gt",
    "greater_than": "gt",
    ">": "gt",
}

_FILTER_LOGIC_ALIASES: dict[str, str] = {
    "and": "and",
    "all": "and",
    "or": "or",
    "any": "or",
}


@dataclass(frozen=True)
class CompiledFilter:
    column: str
    operator: str
    text_values: tuple[str, ...] = ()
    numeric_value: float | None = None


@dataclass(frozen=True)
class FilterGroup:
    """Represents a group of filters or nested groups combined with AND/OR logic."""
    logic: str
    items: tuple[Union[CompiledFilter, FilterGroup], ...] = ()  # Recursive: filters or subgroups
    negate: bool = False  # When True, invert the result of this group


def normalize_operator(operator: Any) -> str:
    normalized = str(operator or "contains").strip().lower()
    if normalized not in _OPERATOR_ALIASES:
        raise ValueError(f"Unsupported filter operator: {operator}")
    return _OPERATOR_ALIASES[normalized]


def normalize_filter_logic(filter_logic: Any) -> str:
    normalized = str(filter_logic or "and").strip().lower()
    if normalized not in _FILTER_LOGIC_ALIASES:
        raise ValueError(f"Unsupported filter logic: {filter_logic}")
    return _FILTER_LOGIC_ALIASES[normalized]


def _normalize_text_values(value: Any) -> tuple[str, ...]:
    raw_values: Iterable[Any]
    if isinstance(value, list):
        raw_values = value
    else:
        raw_values = [value]

    return tuple(str(item or "").strip().lower() for item in raw_values if str(item or "").strip())


def _coerce_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value or "").strip()
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def compile_filter_spec(spec: dict[str, Any]) -> CompiledFilter:
    column = str(spec.get("column", "")).strip()
    if not column:
        raise ValueError("Filter column is required")

    operator = normalize_operator(spec.get("operator"))

    if operator == "missing":
        return CompiledFilter(column=column, operator=operator)

    if operator in ("contains", "not_contains", "is_one_of", "is_not_one_of"):
        text_values = _normalize_text_values(spec.get("value"))
        if not text_values:
            raise ValueError(f"Filter value is required for column: {column}")
        return CompiledFilter(column=column, operator=operator, text_values=text_values)

    numeric_value = _coerce_number(spec.get("value"))
    if numeric_value is None:
        raise ValueError(f"Numeric filter value is required for column: {column}")
    return CompiledFilter(column=column, operator=operator, numeric_value=numeric_value)


def compile_filters(filter_specs: Sequence[dict[str, Any]]) -> list[CompiledFilter]:
    return [compile_filter_spec(spec) for spec in filter_specs]


def _is_missing_value(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def row_matches_filter(row: dict[str, Any], filter_spec: CompiledFilter) -> bool:
    cell_value = row.get(filter_spec.column)

    if filter_spec.operator == "missing":
        return _is_missing_value(cell_value)

    if filter_spec.operator == "contains":
        normalized_cell = str(cell_value or "").lower()
        return any(tv in normalized_cell for tv in filter_spec.text_values)

    if filter_spec.operator == "not_contains":
        normalized_cell = str(cell_value or "").lower()
        return not any(tv in normalized_cell for tv in filter_spec.text_values)

    if filter_spec.operator == "is_one_of":
        normalized_cell = str(cell_value or "").strip().lower()
        return normalized_cell in filter_spec.text_values

    if filter_spec.operator == "is_not_one_of":
        normalized_cell = str(cell_value or "").strip().lower()
        return normalized_cell not in filter_spec.text_values

    numeric_cell = _coerce_number(cell_value)
    if numeric_cell is None:
        return False

    if filter_spec.operator == "lt":
        return numeric_cell < float(filter_spec.numeric_value)  # type: ignore[arg-type]
    if filter_spec.operator == "gt":
        return numeric_cell > float(filter_spec.numeric_value)  # type: ignore[arg-type]
    return False


def row_matches_filter_or_group(row: dict[str, Any], item: Union[CompiledFilter, FilterGroup]) -> bool:
    """Recursively evaluate a filter or filter group against a row."""
    if isinstance(item, CompiledFilter):
        return row_matches_filter(row, item)
    # It's a FilterGroup
    if item.logic == "or":
        result = any(row_matches_filter_or_group(row, sub_item) for sub_item in item.items)
    else:
        result = all(row_matches_filter_or_group(row, sub_item) for sub_item in item.items)
    return not result if item.negate else result


def filter_row_indexes(
    rows: Sequence[dict[str, Any]],
    filter_specs: Sequence[dict[str, Any]],
    combine_with: str = "and",
) -> list[int]:
    """Evaluate a flat list of filter specs with a global combine operator (legacy API)."""
    logic = normalize_filter_logic(combine_with)
    compiled = compile_filters(filter_specs)
    if not compiled:
        return list(range(len(rows)))
    group = FilterGroup(logic=logic, items=tuple(compiled))
    return [i for i, row in enumerate(rows) if row_matches_filter_or_group(row, group)]


def filter_row_indexes_by_group(
    rows: Sequence[dict[str, Any]],
    filter_group: FilterGroup,
) -> list[int]:
    """Evaluate a hierarchical FilterGroup structure."""
    if not filter_group.items:
        return list(range(len(rows)))
    return [i for i, row in enumerate(rows) if row_matches_filter_or_group(row, filter_group)]


# Backward compatibility: legacy flat-list filter evaluation
def row_matches_filters(
    row: dict[str, Any],
    filters: Sequence[CompiledFilter],
    combine_with: str = "and",
) -> bool:
    """Legacy: evaluate a flat list of compiled filters with a single combine operator."""
    logic = normalize_filter_logic(combine_with)
    if not filters:
        return True
    group = FilterGroup(logic=logic, items=tuple(filters))
    return row_matches_filter_or_group(row, group)


def filter_rows(
    rows: Sequence[dict[str, Any]],
    filter_specs: Sequence[dict[str, Any]],
    combine_with: str = "and",
) -> list[dict[str, Any]]:
    return [rows[i] for i in filter_row_indexes(rows, filter_specs, combine_with=combine_with)]


def filter_rows_by_group(
    rows: Sequence[dict[str, Any]],
    filter_group: FilterGroup,
) -> list[dict[str, Any]]:
    return [rows[i] for i in filter_row_indexes_by_group(rows, filter_group)]
