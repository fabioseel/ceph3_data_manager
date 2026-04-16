"""Read and write filter settings YAML files.

The canonical YAML format is::

    bucket: "mybucket"
    prefix: "my/prefix"
    filters:
      - tag: "baseline"
      - score: { op: gt, value: "0.9" }
      - notes: { op: missing }
      - label: { op: not_contains, value: "debug" }
    matching_experiments:
      - s3://mybucket/my/prefix/run-1
      - s3://mybucket/my/prefix/run-2

``filters`` entries are list items, each a single-key mapping.  The value is
either a plain scalar / list (shorthand for ``contains``), or an inline mapping
with ``op`` (required) and ``value`` (required for all operators except
``missing``).
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterator, Sequence, TypedDict

import yaml

from experiment_filters._core import compile_filters


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class FilterSpec(TypedDict, total=False):
    column: str
    operator: str
    value: Any


class FilterSettings(TypedDict, total=False):
    bucket: str
    prefix: str
    filters: list[FilterSpec]
    matching_experiments: list[str]


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------


def _yaml_quote(value: str) -> str:
    """Return *value* serialised as a double-quoted YAML scalar."""
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _serialize_filter_value(spec: FilterSpec) -> str:
    """Return the YAML value fragment for one filter spec dict."""
    op = str(spec.get("operator", "contains")).lower()
    value = spec.get("value")

    if op == "missing":
        return "{ op: missing }"

    if op in ("lt", "gt", "not_contains"):
        return f"{{ op: {op}, value: {_yaml_quote(str(value))} }}"

    # contains — plain scalar or list
    if isinstance(value, list):
        items = ", ".join(_yaml_quote(str(v)) for v in value)
        return f"[{items}]"
    return _yaml_quote(str(value))


# ---------------------------------------------------------------------------
# Parse helpers
# ---------------------------------------------------------------------------


def _unquote(value: str) -> str:
    t = value.strip()
    if len(t) >= 2 and t[0] == '"' and t[-1] == '"':
        return t[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    if len(t) >= 2 and t[0] == "'" and t[-1] == "'":
        return t[1:-1]
    return t


def _split_csv(inner: str) -> list[str]:
    """Split a comma-separated string, respecting quotes."""
    parts: list[str] = []
    current = ""
    quote: str | None = None
    for ch in inner:
        if ch in ('"', "'") and quote is None:
            quote = ch
        elif ch == quote:
            quote = None
        elif ch == "," and quote is None:
            parts.append(_unquote(current.strip()))
            current = ""
            continue
        current += ch
    if current.strip():
        parts.append(_unquote(current.strip()))
    return parts


def _parse_inline_list(raw: str) -> list[str] | None:
    t = raw.strip()
    if t.startswith("[") and t.endswith("]"):
        inner = t[1:-1].strip()
        return _split_csv(inner) if inner else []
    return None


def _parse_inline_mapping(raw: str) -> dict[str, Any] | None:
    """Parse ``{ key: val, key2: val2 }`` → dict.  Returns *None* on mismatch."""
    t = raw.strip()
    if not (t.startswith("{") and t.endswith("}")):
        return None
    inner = t[1:-1].strip()
    if not inner:
        return {}
    result: dict[str, Any] = {}
    for entry in _split_csv(inner):
        colon = entry.find(":")
        if colon == -1:
            continue
        k = entry[:colon].strip()
        v_raw = entry[colon + 1:].strip()
        v_list = _parse_inline_list(v_raw)
        result[k] = v_list if v_list is not None else _unquote(v_raw)
    return result


def _parse_filter_entry(entry_text: str, known_columns: set[str] | None = None) -> FilterSpec | None:
    """Parse one ``column: value`` entry from the filters list."""
    m = re.match(r"^([A-Za-z0-9_.\[\]-]+):\s*(.*)$", entry_text)
    if not m:
        return None
    column, raw_value = m.group(1), m.group(2)
    if known_columns is not None and column not in known_columns:
        return None

    mapping = _parse_inline_mapping(raw_value)
    if mapping is not None:
        op = str(mapping.get("op") or mapping.get("operator") or "contains")
        value = mapping.get("value")
        spec: FilterSpec = {"column": column, "operator": op}
        if op != "missing":
            spec["value"] = value
        return spec

    parsed_list = _parse_inline_list(raw_value)
    value_or_list: Any = parsed_list if parsed_list is not None else _unquote(raw_value)
    return {"column": column, "operator": "contains", "value": value_or_list}


def _iter_filter_specs(text: str, known_columns: set[str] | None = None) -> Iterator[FilterSpec]:
    """Yield FilterSpec dicts from the ``filters:`` section of *text*."""
    in_filters = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line == "filters:":
            in_filters = True
            continue
        if in_filters and line in ("[]",):
            in_filters = False
            continue
        # Any top-level key other than list items ends the filters section.
        if in_filters and not line.startswith("-") and ":" in line and not line.startswith(" "):
            in_filters = False
        if in_filters and line.startswith("-"):
            entry = line.lstrip("- ").strip()
            spec = _parse_filter_entry(entry, known_columns)
            if spec:
                yield spec


def _iter_matching_experiments(text: str) -> Iterator[str]:
    in_section = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line in ("matching_experiments:", "experiments:"):
            in_section = True
            continue
        if in_section and line in ("[]",):
            in_section = False
            continue
        if in_section and not line.startswith("-"):
            in_section = False
        if in_section and line.startswith("-"):
            yield _unquote(line[1:].strip())


def _scalar(text: str, key: str) -> str:
    """Extract a top-level scalar value from a minimal YAML text."""
    m = re.search(rf'^{re.escape(key)}:\s*(.+)$', text, re.MULTILINE)
    if not m:
        return ""
    return _unquote(m.group(1).strip())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_filter_settings(
    text: str,
    known_columns: Sequence[str] | None = None,
    validate: bool = True,
) -> FilterSettings:
    """Parse a YAML string into a :class:`FilterSettings` dict.

    Parameters
    ----------
    text:
        Raw YAML text.
    known_columns:
        When supplied, filter entries whose column is not in this set are
        silently dropped.
    validate:
        When *True* (default) the parsed filter specs are forwarded to
        :func:`compile_filters` so that invalid operators / missing values
        raise :class:`ValueError` early.
    """
    col_set = set(known_columns) if known_columns is not None else None
    filters = list(_iter_filter_specs(text, col_set))
    if validate and filters:
        compile_filters(filters)  # raises ValueError on bad specs

    return FilterSettings(
        bucket=_scalar(text, "bucket"),
        prefix=_scalar(text, "prefix"),
        filters=filters,
        matching_experiments=list(_iter_matching_experiments(text)),
    )


def load_filter_settings(
    path: str | Path,
    known_columns: Sequence[str] | None = None,
    validate: bool = True,
) -> FilterSettings:
    """Load :class:`FilterSettings` from a YAML file on disk."""
    text = Path(path).read_text(encoding="utf-8")
    return parse_filter_settings(text, known_columns=known_columns, validate=validate)


def dump_filter_settings(
    settings: FilterSettings,
    matching_experiments: Sequence[str] | None = None,
) -> str:
    """Serialise *settings* to a YAML string.

    Parameters
    ----------
    settings:
        The settings to serialise.  ``matching_experiments`` inside *settings*
        is used unless the *matching_experiments* argument is also supplied, in
        which case the argument takes precedence.
    """
    lines: list[str] = []
    bucket = settings.get("bucket", "")
    prefix = settings.get("prefix", "")
    lines.append(f"bucket: {_yaml_quote(bucket)}")
    lines.append(f"prefix: {_yaml_quote(prefix)}")

    filters = settings.get("filters") or []
    lines.append("filters:")
    if filters:
        for spec in filters:
            column = spec["column"]
            lines.append(f"  - {column}: {_serialize_filter_value(spec)}")
    else:
        lines.append("  []")

    exps = matching_experiments if matching_experiments is not None else settings.get("matching_experiments") or []
    lines.append("matching_experiments:")
    if exps:
        for path in exps:
            lines.append(f"  - {_yaml_quote(path)}")
    else:
        lines.append("  []")

    return "\n".join(lines) + "\n"
