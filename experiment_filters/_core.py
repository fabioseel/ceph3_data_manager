from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence


_OPERATOR_ALIASES: dict[str, str] = {
    "contains": "contains",
    "match": "contains",
    "matches": "contains",
    "not_contains": "not_contains",
    "does_not_contain": "not_contains",
    "not_match": "not_contains",
    "not_matches": "not_contains",
    "!contains": "not_contains",
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


@dataclass(frozen=True)
class CompiledFilter:
    column: str
    operator: str
    text_values: tuple[str, ...] = ()
    numeric_value: float | None = None


def normalize_operator(operator: Any) -> str:
    normalized = str(operator or "contains").strip().lower()
    if normalized not in _OPERATOR_ALIASES:
        raise ValueError(f"Unsupported filter operator: {operator}")
    return _OPERATOR_ALIASES[normalized]


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

    if operator in ("contains", "not_contains"):
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


def row_matches_filters(row: dict[str, Any], filters: Sequence[CompiledFilter]) -> bool:
    for f in filters:
        cell_value = row.get(f.column)

        if f.operator == "missing":
            if not _is_missing_value(cell_value):
                return False
            continue

        if f.operator == "contains":
            normalized_cell = str(cell_value or "").lower()
            if not any(tv in normalized_cell for tv in f.text_values):
                return False
            continue

        if f.operator == "not_contains":
            normalized_cell = str(cell_value or "").lower()
            if any(tv in normalized_cell for tv in f.text_values):
                return False
            continue

        numeric_cell = _coerce_number(cell_value)
        if numeric_cell is None:
            return False

        if f.operator == "lt" and not numeric_cell < float(f.numeric_value):  # type: ignore[arg-type]
            return False
        if f.operator == "gt" and not numeric_cell > float(f.numeric_value):  # type: ignore[arg-type]
            return False

    return True


def filter_row_indexes(
    rows: Sequence[dict[str, Any]], filter_specs: Sequence[dict[str, Any]]
) -> list[int]:
    compiled = compile_filters(filter_specs)
    if not compiled:
        return list(range(len(rows)))
    return [i for i, row in enumerate(rows) if row_matches_filters(row, compiled)]


def filter_rows(
    rows: Sequence[dict[str, Any]], filter_specs: Sequence[dict[str, Any]]
) -> list[dict[str, Any]]:
    return [rows[i] for i in filter_row_indexes(rows, filter_specs)]
