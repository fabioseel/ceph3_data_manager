import os
from datetime import datetime
from configparser import ConfigParser
from dataclasses import dataclass
from threading import Lock, Thread
from typing import Any
from uuid import uuid4

import boto3
import botocore
from botocore.config import Config
from experiment_filters import filter_row_indexes
from flask import Flask, jsonify, render_template, request
from omegaconf import OmegaConf
import yaml


app = Flask(__name__)
JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = Lock()


@dataclass
class ExperimentConfigFiles:
    experiment_id: str
    config_key: str | None = None
    hydra_key: str | None = None
    overrides_key: str | None = None


def load_s3cmd_settings(path: str | None = None) -> dict[str, str]:
    """Load relevant settings from s3cmd config (~/.s3cfg by default)."""
    config_path = path or os.path.expanduser("~/.s3cfg")
    if not os.path.exists(config_path):
        return {}

    parser = ConfigParser()
    parser.read(config_path)

    # s3cmd files can store values in parser defaults or in a [default] section.
    section = parser.defaults()
    if not section and parser.has_section("default"):
        section = parser["default"]
    if not section and parser.sections():
        section = parser[parser.sections()[0]]

    return {
        "access_key": section.get("access_key", "").strip(),
        "secret_key": section.get("secret_key", "").strip(),
        "host_base": section.get("host_base", "").strip(),
        "use_https": section.get("use_https", "").strip().lower(),
    }


def flatten_dict(data: Any, parent_key: str = "") -> dict[str, Any]:
    """Flatten nested dict/list structures using dot paths and list indexes."""
    flat: dict[str, Any] = {}

    if isinstance(data, dict):
        for key, value in data.items():
            child_key = f"{parent_key}.{key}" if parent_key else str(key)
            flat.update(flatten_dict(value, child_key))
        return flat

    if isinstance(data, list):
        for index, value in enumerate(data):
            child_key = f"{parent_key}[{index}]" if parent_key else f"[{index}]"
            flat.update(flatten_dict(value, child_key))
        return flat

    leaf_key = parent_key or "value"
    flat[leaf_key] = data
    return flat


def is_yaml_key(object_key: str) -> bool:
    return object_key.lower().endswith((".yaml", ".yml"))


def iter_yaml_keys(s3_client: Any, bucket: str, prefix: str = "") -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    params = {"Bucket": bucket}
    if prefix:
        params["Prefix"] = prefix

    keys: list[str] = []
    for page in paginator.paginate(**params):
        for item in page.get("Contents", []):
            key = item.get("Key", "")
            if is_yaml_key(key):
                keys.append(key)
    return keys


def read_yaml_from_s3(s3_client: Any, bucket: str, key: str) -> Any:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return yaml.safe_load(body)


def normalize_override(override: str) -> str:
    value = override.strip()
    if value.startswith("+"):
        value = value[1:]
    if value.startswith("~"):
        value = value[1:]
    return value


def experiment_id_from_key(key: str) -> str:
    lower_key = key.lower()
    for marker in ("/.hydra/", "/hydra/", "/config/"):
        marker_index = lower_key.rfind(marker)
        if marker_index != -1:
            return key[:marker_index]
    return key.rsplit("/", 1)[0] if "/" in key else key


def display_experiment_id(experiment_id: str) -> str:
    normalized = experiment_id.rstrip("/")
    if "/" not in normalized:
        return normalized
    return normalized.rsplit("/", 1)[-1]


def collect_experiment_files(keys: list[str]) -> dict[str, ExperimentConfigFiles]:
    experiments: dict[str, ExperimentConfigFiles] = {}

    for key in keys:
        lower_key = key.lower()
        if not lower_key.endswith(".yaml") and not lower_key.endswith(".yml"):
            continue

        # Only consider Hydra config bundles inside known config directories.
        if "/.hydra/" not in lower_key and "/hydra/" not in lower_key and "/config/" not in lower_key:
            continue

        filename = lower_key.rsplit("/", 1)[-1]
        if filename not in {"config.yaml", "hydra.yaml", "overrides.yaml"}:
            continue

        experiment_id = experiment_id_from_key(key)
        entry = experiments.get(experiment_id)
        if entry is None:
            entry = ExperimentConfigFiles(experiment_id=experiment_id)
            experiments[experiment_id] = entry

        if filename == "config.yaml":
            entry.config_key = key
        elif filename == "hydra.yaml":
            entry.hydra_key = key
        elif filename == "overrides.yaml":
            entry.overrides_key = key

    return experiments


def register_interpolation_resolvers(config: Any) -> None:
    def hydra_resolver(path: str) -> Any:
        return OmegaConf.select(config, f"hydra.{path}")

    def now_resolver(fmt: str = "%Y-%m-%d") -> str:
        return datetime.now().strftime(fmt)

    def eval_resolver(expr: str) -> Any:
        safe_globals = {"__builtins__": {}}
        return eval(expr, safe_globals, {})  # noqa: S307

    OmegaConf.register_new_resolver("hydra", hydra_resolver, replace=True)
    OmegaConf.register_new_resolver("now", now_resolver, replace=True)
    OmegaConf.register_new_resolver("eval", eval_resolver, replace=True)


def create_s3_client(region: str | None = None, endpoint_url: str | None = None) -> Any:
    s3cmd_settings = load_s3cmd_settings()

    effective_endpoint = endpoint_url
    if not effective_endpoint and s3cmd_settings.get("host_base"):
        scheme = "https" if s3cmd_settings.get("use_https") in {"true", "yes", "1"} else "http"
        effective_endpoint = f"{scheme}://{s3cmd_settings['host_base']}"

    # Ceph compatibility defaults: path-style bucket addressing and checksum behavior.
    config = Config(
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )

    client_kwargs: dict[str, Any] = {
        "region_name": region or "us-east-1",
        "config": config,
    }
    if effective_endpoint:
        client_kwargs["endpoint_url"] = effective_endpoint
    if s3cmd_settings.get("access_key"):
        client_kwargs["aws_access_key_id"] = s3cmd_settings["access_key"]
    if s3cmd_settings.get("secret_key"):
        client_kwargs["aws_secret_access_key"] = s3cmd_settings["secret_key"]

    return boto3.client("s3", **client_kwargs)


def merge_experiment_config(
    s3_client: Any,
    bucket: str,
    files: ExperimentConfigFiles,
) -> dict[str, Any]:
    config_obj = OmegaConf.create({})
    hydra_obj = OmegaConf.create({})
    overrides_obj = OmegaConf.create({})

    if files.config_key:
        config_data = read_yaml_from_s3(s3_client, bucket=bucket, key=files.config_key) or {}
        config_obj = OmegaConf.create(config_data)

    if files.hydra_key:
        hydra_data = read_yaml_from_s3(s3_client, bucket=bucket, key=files.hydra_key) or {}
        if isinstance(hydra_data, dict) and "hydra" in hydra_data:
            hydra_obj = OmegaConf.create(hydra_data)
        else:
            hydra_obj = OmegaConf.create({"hydra": hydra_data})

    if files.overrides_key:
        overrides_data = read_yaml_from_s3(s3_client, bucket=bucket, key=files.overrides_key) or []
        if isinstance(overrides_data, list):
            normalized = [normalize_override(str(item)) for item in overrides_data if str(item).strip()]
            if normalized:
                overrides_obj = OmegaConf.from_dotlist(normalized)

    merged = OmegaConf.merge(config_obj, hydra_obj, overrides_obj)
    register_interpolation_resolvers(merged)

    try:
        resolved = OmegaConf.to_container(merged, resolve=True)
    except Exception:
        # Fall back to unresolved strings when full resolver coverage is not available.
        resolved = OmegaConf.to_container(merged, resolve=False)

    if isinstance(resolved, dict):
        return flatten_dict(resolved)
    return flatten_dict({"value": resolved})


def load_experiment_rows(
    bucket: str,
    prefix: str = "",
    region: str | None = None,
    endpoint_url: str | None = None,
    progress_callback: Any | None = None,
    should_cancel: Any | None = None,
) -> tuple[list[dict[str, str]], list[str], str | None]:
    try:
        s3_client = create_s3_client(region=region, endpoint_url=endpoint_url)
        keys = iter_yaml_keys(s3_client, bucket=bucket, prefix=prefix)
        experiments = collect_experiment_files(keys)

        total = len(experiments)
        processed = 0
        rows: list[dict[str, str]] = []
        all_columns: set[str] = {"experiment_id"}

        if progress_callback:
            progress_callback("loading", processed, total)

        for experiment_id in sorted(experiments.keys()):
            if should_cancel and should_cancel():
                return [], [], "Cancelled"

            files = experiments[experiment_id]
            row: dict[str, str] = {
                "experiment_id": display_experiment_id(experiment_id),
                "__s3_path": experiment_id,
            }
            try:
                flat = merge_experiment_config(s3_client, bucket=bucket, files=files)
                for key, value in flat.items():
                    row[key] = str(value)
                    all_columns.add(key)
            except Exception as file_error:
                row["__error__"] = f"Could not merge config: {file_error}"
                all_columns.add("__error__")

            rows.append(row)
            processed += 1
            if progress_callback:
                progress_callback("loading", processed, total)

        ordered_columns = ["experiment_id"] + sorted(c for c in all_columns if c != "experiment_id")
        return rows, ordered_columns, None

    except botocore.exceptions.BotoCoreError as error:
        return [], [], f"AWS SDK error: {error}"
    except botocore.exceptions.ClientError as error:
        return [], [], f"AWS client error: {error}"
    except Exception as error:
        return [], [], f"Unexpected error: {error}"


def set_job_state(job_id: str, **updates: Any) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            return
        job.update(updates)


def get_job_state(job_id: str) -> dict[str, Any] | None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            return None
        return dict(job)


def run_load_job(job_id: str, bucket: str, prefix: str, region: str | None, endpoint_url: str | None) -> None:
    def progress_callback(phase: str, processed: int, total: int) -> None:
        set_job_state(job_id, phase=phase, processed=processed, total=total)

    def should_cancel() -> bool:
        state = get_job_state(job_id)
        return bool(state and state.get("cancel_requested"))

    rows, columns, error = load_experiment_rows(
        bucket=bucket,
        prefix=prefix,
        region=region,
        endpoint_url=endpoint_url,
        progress_callback=progress_callback,
        should_cancel=should_cancel,
    )

    if error == "Cancelled":
        set_job_state(job_id, status="cancelled", error="Loading cancelled by user")
        return

    if error:
        set_job_state(job_id, status="error", error=error)
        return

    set_job_state(job_id, status="done", rows=rows, columns=columns, processed=len(rows), total=len(rows))


@app.route("/", methods=["GET"])
def index() -> str:
    bucket = request.args.get("bucket", os.getenv("S3_BUCKET", "")).strip()
    prefix = request.args.get("prefix", os.getenv("S3_PREFIX", "")).strip()
    region = request.args.get("region", os.getenv("AWS_REGION", "")).strip() or None
    endpoint_url = request.args.get("endpoint_url", os.getenv("S3_ENDPOINT_URL", "")).strip() or None

    return render_template(
        "index.html",
        bucket=bucket,
        prefix=prefix,
        region=region or "",
        endpoint_url=endpoint_url or "",
    )


@app.route("/api/load", methods=["POST"])
def api_load() -> Any:
    payload = request.get_json(silent=True) or {}
    bucket = str(payload.get("bucket", "")).strip()
    prefix = str(payload.get("prefix", "")).strip()
    region = str(payload.get("region", "")).strip() or None
    endpoint_url = str(payload.get("endpoint_url", "")).strip() or None

    if not bucket:
        return jsonify({"error": "Bucket is required"}), 400

    job_id = uuid4().hex
    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "running",
            "phase": "starting",
            "processed": 0,
            "total": 0,
            "error": None,
            "rows": [],
            "columns": [],
            "cancel_requested": False,
        }

    worker = Thread(target=run_load_job, args=(job_id, bucket, prefix, region, endpoint_url), daemon=True)
    worker.start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>", methods=["GET"])
def api_status(job_id: str) -> Any:
    state = get_job_state(job_id)
    if state is None:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(state)


@app.route("/api/filter", methods=["POST"])
def api_filter() -> Any:
    payload = request.get_json(silent=True) or {}
    job_id = str(payload.get("job_id", "")).strip()
    filter_specs = payload.get("filters") or []
    ordered_paths = payload.get("ordered_paths") or []

    if not job_id:
        return jsonify({"error": "job_id is required"}), 400
    if not isinstance(filter_specs, list):
        return jsonify({"error": "filters must be a list"}), 400
    if not isinstance(ordered_paths, list):
        return jsonify({"error": "ordered_paths must be a list"}), 400

    state = get_job_state(job_id)
    if state is None:
        return jsonify({"error": "Job not found"}), 404
    if state.get("status") != "done":
        return jsonify({"error": "Job is not ready for filtering"}), 409

    rows = state.get("rows") or []
    ordered_rows = rows
    if ordered_paths:
        rows_by_path = {str(row.get("__s3_path", "")): row for row in rows if row.get("__s3_path")}
        ordered_rows = [rows_by_path[path] for path in map(str, ordered_paths) if path in rows_by_path]

    try:
        matching_indexes = filter_row_indexes(ordered_rows, filter_specs)
    except ValueError as error:
        return jsonify({"error": str(error)}), 400

    return jsonify({"matching_indexes": matching_indexes})


@app.route("/api/abort/<job_id>", methods=["POST"])
def api_abort(job_id: str) -> Any:
    state = get_job_state(job_id)
    if state is None:
        return jsonify({"error": "Job not found"}), 404

    set_job_state(job_id, cancel_requested=True)
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
