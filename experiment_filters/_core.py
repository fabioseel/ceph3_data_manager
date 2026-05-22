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
    "in_other_column": "in_other_column",
    "in other column": "in_other_column",
    "partially_in_other_column": "partially_in_other_column",
    "partially in other column": "partially_in_other_column",
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
    use_visible_rows_only: bool = False
    disabled: bool = False
    negate: bool = False


@dataclass(frozen=True)
class FilterGroup:
    """Represents a group of filters or nested groups combined with AND/OR logic."""
    logic: str
    items: tuple[Union[CompiledFilter, FilterGroup], ...] = ()  # Recursive: filters or subgroups
    negate: bool = False  # When True, invert the result of this group
    disabled: bool = False


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


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def compile_filter_spec(spec: dict[str, Any]) -> CompiledFilter:
    column = str(spec.get("column", "")).strip()
    if not column:
        raise ValueError("Filter column is required")

    disabled = _coerce_bool(spec.get("disabled", False))
    negate = _coerce_bool(spec.get("negate", False))
    operator = normalize_operator(spec.get("operator"))

    if operator == "missing":
        return CompiledFilter(column=column, operator=operator, disabled=disabled, negate=negate)

    if operator in ("in_other_column", "partially_in_other_column"):
        other_column = str(spec.get("other_column") or spec.get("value") or "").strip()
        if not other_column:
            raise ValueError(f"Target column is required for {operator} filter: {column}")
        return CompiledFilter(
            column=column,
            operator=operator,
            text_values=(other_column,),
            use_visible_rows_only=_coerce_bool(spec.get("use_visible_rows_only", False)),
            disabled=disabled,
            negate=negate,
        )

    if operator in ("contains", "not_contains", "is_one_of", "is_not_one_of"):
        text_values = _normalize_text_values(spec.get("value"))
        if not text_values:
            raise ValueError(f"Filter value is required for column: {column}")
        return CompiledFilter(column=column, operator=operator, text_values=text_values, disabled=disabled, negate=negate)

    numeric_value = _coerce_number(spec.get("value"))
    if numeric_value is None:
        raise ValueError(f"Numeric filter value is required for column: {column}")
    return CompiledFilter(column=column, operator=operator, numeric_value=numeric_value, disabled=disabled, negate=negate)


def compile_filters(filter_specs: Sequence[dict[str, Any]]) -> list[CompiledFilter]:
    return [compile_filter_spec(spec) for spec in filter_specs]


def _is_missing_value(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _normalized_cell_value(value: Any) -> str:
    return str(value or "").strip().lower()


def _collect_compiled_filters(item: Union[CompiledFilter, FilterGroup]) -> list[CompiledFilter]:
    if isinstance(item, CompiledFilter):
        return [] if item.disabled else [item]
    if item.disabled:
        return []
    filters: list[CompiledFilter] = []
    for sub_item in item.items:
        filters.extend(_collect_compiled_filters(sub_item))
    return filters


def _rows_from_indexes(rows: Sequence[dict[str, Any]], row_indexes: Sequence[int] | None) -> Sequence[dict[str, Any]]:
    if row_indexes is None:
        return rows
    subset: list[dict[str, Any]] = []
    for index in row_indexes:
        if isinstance(index, int) and 0 <= index < len(rows):
            subset.append(rows[index])
    return subset


def _build_in_other_column_lookup_sets(
    rows: Sequence[dict[str, Any]],
    filter_item: Union[CompiledFilter, FilterGroup],
    visible_row_indexes: Sequence[int] | None,
) -> dict[CompiledFilter, set[str]]:
    lookup_sets: dict[CompiledFilter, set[str]] = {}
    for compiled_filter in _collect_compiled_filters(filter_item):
        if compiled_filter.operator not in ("in_other_column", "partially_in_other_column"):
            continue
        target_column = compiled_filter.text_values[0]
        source_rows = _rows_from_indexes(rows, visible_row_indexes) if compiled_filter.use_visible_rows_only else rows
        values = {
            normalized
            for normalized in (_normalized_cell_value(row.get(target_column)) for row in source_rows)
            if normalized
        }
        lookup_sets[compiled_filter] = values
    return lookup_sets


def row_matches_filter(
    row: dict[str, Any],
    filter_spec: CompiledFilter,
    in_other_column_lookups: dict[CompiledFilter, set[str]] | None = None,
) -> bool:
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
        normalized_cell = _normalized_cell_value(cell_value)
        return normalized_cell not in filter_spec.text_values

    if filter_spec.operator == "in_other_column":
        normalized_cell = _normalized_cell_value(cell_value)
        if not normalized_cell:
            return False
        lookup_values = (in_other_column_lookups or {}).get(filter_spec, set())
        return normalized_cell in lookup_values

    if filter_spec.operator == "partially_in_other_column":
        normalized_cell = _normalized_cell_value(cell_value)
        if not normalized_cell:
            return False
        lookup_values = (in_other_column_lookups or {}).get(filter_spec, set())
        return any(normalized_cell in candidate for candidate in lookup_values)

    numeric_cell = _coerce_number(cell_value)
    if numeric_cell is None:
        return False

    if filter_spec.operator == "lt":
        return numeric_cell < float(filter_spec.numeric_value)  # type: ignore[arg-type]
    if filter_spec.operator == "gt":
        return numeric_cell > float(filter_spec.numeric_value)  # type: ignore[arg-type]
    return False


def row_matches_filter_or_group(
    row: dict[str, Any],
    item: Union[CompiledFilter, FilterGroup],
    in_other_column_lookups: dict[CompiledFilter, set[str]] | None = None,
) -> bool:
    """Recursively evaluate a filter or filter group against a row."""
    if isinstance(item, CompiledFilter):
        if item.disabled:
            return True
        result = row_matches_filter(row, item, in_other_column_lookups=in_other_column_lookups)
        return not result if item.negate else result
    # It's a FilterGroup
    if item.disabled:
        return True
    active_items = [i for i in item.items if not i.disabled]
    if not active_items:
        result = True
    elif item.logic == "or":
        result = any(
            row_matches_filter_or_group(row, sub_item, in_other_column_lookups=in_other_column_lookups)
            for sub_item in active_items
        )
    else:
        result = all(
            row_matches_filter_or_group(row, sub_item, in_other_column_lookups=in_other_column_lookups)
            for sub_item in active_items
        )
    return not result if item.negate else result


def filter_row_indexes(
    rows: Sequence[dict[str, Any]],
    filter_specs: Sequence[dict[str, Any]],
    combine_with: str = "and",
    visible_row_indexes: Sequence[int] | None = None,
) -> list[int]:
    """Evaluate a flat list of filter specs with a global combine operator (legacy API)."""
    logic = normalize_filter_logic(combine_with)
    compiled = compile_filters(filter_specs)
    if not compiled:
        return list(range(len(rows)))
    group = FilterGroup(logic=logic, items=tuple(compiled))
    return filter_row_indexes_by_group(rows, group, visible_row_indexes)


def _filter_and_sequential(
    rows: Sequence[dict[str, Any]],
    group: FilterGroup,
) -> list[int]:
    """Apply AND filters one by one so use_visible_rows_only filters use preceding filter results as context."""
    active_items = [item for item in group.items if not item.disabled]
    if not active_items:
        result_indexes = list(range(len(rows)))
    else:
        current_result = list(range(len(rows)))
        for item in active_items:
            if isinstance(item, FilterGroup):
                sub_result_set = set(filter_row_indexes_by_group(rows, item, current_result))
                current_result = [i for i in current_result if i in sub_result_set]
            else:
                if item.use_visible_rows_only and item.operator in ("in_other_column", "partially_in_other_column"):
                    target_col = item.text_values[0]
                    lookup_set = {
                        v for v in (
                            _normalized_cell_value(rows[i].get(target_col))
                            for i in current_result
                        )
                        if v
                    }
                    item_lookups: dict[CompiledFilter, set[str]] = {item: lookup_set}
                else:
                    item_lookups = _build_in_other_column_lookup_sets(rows, item, None)
                current_result = [
                    i for i in current_result
                    if row_matches_filter(rows[i], item, item_lookups) != item.negate
                ]
        result_indexes = current_result

    if group.negate:
        all_indexes = set(range(len(rows)))
        return sorted(all_indexes - set(result_indexes))
    return result_indexes


def filter_row_indexes_by_group(
    rows: Sequence[dict[str, Any]],
    filter_group: FilterGroup,
    visible_row_indexes: Sequence[int] | None = None,
) -> list[int]:
    """Evaluate a hierarchical FilterGroup structure."""
    if not filter_group.items or filter_group.disabled:
        return list(range(len(rows)))
    if filter_group.logic == "and":
        return _filter_and_sequential(rows, filter_group)
    lookup_sets = _build_in_other_column_lookup_sets(rows, filter_group, visible_row_indexes)
    return [
        i
        for i, row in enumerate(rows)
        if row_matches_filter_or_group(row, filter_group, in_other_column_lookups=lookup_sets)
    ]


# Backward compatibility: legacy flat-list filter evaluation
def row_matches_filters(
    row: dict[str, Any],
    filters: Sequence[CompiledFilter],
    combine_with: str = "and",
    all_rows: Sequence[dict[str, Any]] | None = None,
    visible_row_indexes: Sequence[int] | None = None,
) -> bool:
    """Legacy: evaluate a flat list of compiled filters with a single combine operator."""
    logic = normalize_filter_logic(combine_with)
    if not filters:
        return True
    group = FilterGroup(logic=logic, items=tuple(filters))
    lookup_rows = all_rows if all_rows is not None else [row]
    lookup_sets = _build_in_other_column_lookup_sets(lookup_rows, group, visible_row_indexes)
    return row_matches_filter_or_group(row, group, in_other_column_lookups=lookup_sets)


def filter_rows(
    rows: Sequence[dict[str, Any]],
    filter_specs: Sequence[dict[str, Any]],
    combine_with: str = "and",
    visible_row_indexes: Sequence[int] | None = None,
) -> list[dict[str, Any]]:
    return [
        rows[i]
        for i in filter_row_indexes(
            rows,
            filter_specs,
            combine_with=combine_with,
            visible_row_indexes=visible_row_indexes,
        )
    ]


def filter_rows_by_group(
    rows: Sequence[dict[str, Any]],
    filter_group: FilterGroup,
    visible_row_indexes: Sequence[int] | None = None,
) -> list[dict[str, Any]]:
    return [
        rows[i]
        for i in filter_row_indexes_by_group(rows, filter_group, visible_row_indexes=visible_row_indexes)
    ]
