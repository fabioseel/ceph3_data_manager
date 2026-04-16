from __future__ import annotations

from pathlib import Path
from typing import Any

import botocore
import pytest

import app as app_module


def test_flatten_dict_handles_nested_dicts_and_lists() -> None:
    payload = {
        "service": {
            "name": "api",
            "ports": [8080, 8081],
            "flags": {"enabled": True},
        }
    }

    result = app_module.flatten_dict(payload)

    assert result == {
        "service.name": "api",
        "service.ports[0]": 8080,
        "service.ports[1]": 8081,
        "service.flags.enabled": True,
    }


def test_flatten_dict_handles_scalar_root() -> None:
    result = app_module.flatten_dict("hello")
    assert result == {"value": "hello"}


def test_collect_experiment_files_groups_hydra_triplet() -> None:
    keys = [
        "runs/exp-1/.hydra/config.yaml",
        "runs/exp-1/.hydra/hydra.yaml",
        "runs/exp-1/.hydra/overrides.yaml",
        "runs/exp-2/.hydra/config.yaml",
    ]

    experiments = app_module.collect_experiment_files(keys)

    assert "runs/exp-1" in experiments
    assert experiments["runs/exp-1"].config_key == "runs/exp-1/.hydra/config.yaml"
    assert experiments["runs/exp-1"].hydra_key == "runs/exp-1/.hydra/hydra.yaml"
    assert experiments["runs/exp-1"].overrides_key == "runs/exp-1/.hydra/overrides.yaml"
    assert experiments["runs/exp-2"].config_key == "runs/exp-2/.hydra/config.yaml"


def test_collect_experiment_files_groups_config_folder_triplet() -> None:
    keys = [
        "run_2025-11-24-14-54-53-497055/config/config.yaml",
        "run_2025-11-24-14-54-53-497055/config/hydra.yaml",
        "run_2025-11-24-14-54-53-497055/config/overrides.yaml",
        "run_2025-11-24-14-54-53-497055/wandb/wandb/files/config.yaml",
    ]

    experiments = app_module.collect_experiment_files(keys)

    assert "run_2025-11-24-14-54-53-497055" in experiments
    assert experiments["run_2025-11-24-14-54-53-497055"].config_key == "run_2025-11-24-14-54-53-497055/config/config.yaml"
    assert experiments["run_2025-11-24-14-54-53-497055"].hydra_key == "run_2025-11-24-14-54-53-497055/config/hydra.yaml"
    assert experiments["run_2025-11-24-14-54-53-497055"].overrides_key == "run_2025-11-24-14-54-53-497055/config/overrides.yaml"
    assert "run_2025-11-24-14-54-53-497055/wandb/wandb/files" not in experiments


def test_collect_experiment_files_supports_intermediate_directories() -> None:
    keys = [
        "bep224/retinal_rl/experiments/deucalion/SOMEDIR/experiments/run_001/.hydra/config.yaml",
        "bep224/retinal_rl/experiments/deucalion/SOMEDIR/experiments/run_001/.hydra/hydra.yaml",
        "bep224/retinal_rl/experiments/deucalion/SOMEDIR/experiments/run_001/.hydra/overrides.yaml",
    ]

    experiments = app_module.collect_experiment_files(keys)

    experiment_id = "bep224/retinal_rl/experiments/deucalion/SOMEDIR/experiments/run_001"
    assert experiment_id in experiments
    assert experiments[experiment_id].config_key == keys[0]
    assert experiments[experiment_id].hydra_key == keys[1]
    assert experiments[experiment_id].overrides_key == keys[2]


def test_experiment_id_from_key_uses_last_marker_occurrence() -> None:
    key = "root/config/archive/exp/config/config.yaml"
    assert app_module.experiment_id_from_key(key) == "root/config/archive/exp"


def test_display_experiment_id_returns_basename() -> None:
    assert app_module.display_experiment_id("runs/group-a/run_2025-11-24-14-54-53-497055") == "run_2025-11-24-14-54-53-497055"
    assert app_module.display_experiment_id("run_2025-11-24-14-54-53-497055") == "run_2025-11-24-14-54-53-497055"


def test_load_s3cmd_settings_reads_expected_fields(tmp_path: Path) -> None:
    cfg = tmp_path / ".s3cfg"
    cfg.write_text(
        "[default]\n"
        "access_key = abc\n"
        "secret_key = secret\n"
        "host_base = s3.example.local\n"
        "use_https = True\n",
        encoding="utf-8",
    )

    settings = app_module.load_s3cmd_settings(str(cfg))

    assert settings["access_key"] == "abc"
    assert settings["secret_key"] == "secret"
    assert settings["host_base"] == "s3.example.local"
    assert settings["use_https"] == "true"


def test_create_s3_client_uses_s3cfg_endpoint_and_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class DummyClient:
        pass

    def fake_boto3_client(service_name: str, **kwargs: Any) -> DummyClient:
        captured["service_name"] = service_name
        captured["kwargs"] = kwargs
        return DummyClient()

    monkeypatch.setattr(
        app_module,
        "load_s3cmd_settings",
        lambda path=None: {
            "access_key": "abc",
            "secret_key": "secret",
            "host_base": "s3.mlcloud.uni-tuebingen.de",
            "use_https": "true",
        },
    )
    monkeypatch.setattr(app_module.boto3, "client", fake_boto3_client)

    client = app_module.create_s3_client(region=None, endpoint_url=None)

    assert isinstance(client, DummyClient)
    assert captured["service_name"] == "s3"
    assert captured["kwargs"]["endpoint_url"] == "https://s3.mlcloud.uni-tuebingen.de"
    assert captured["kwargs"]["aws_access_key_id"] == "abc"
    assert captured["kwargs"]["aws_secret_access_key"] == "secret"
    assert captured["kwargs"]["region_name"] == "us-east-1"


def test_create_s3_client_prefers_explicit_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class DummyClient:
        pass

    def fake_boto3_client(service_name: str, **kwargs: Any) -> DummyClient:
        captured["kwargs"] = kwargs
        return DummyClient()

    monkeypatch.setattr(
        app_module,
        "load_s3cmd_settings",
        lambda path=None: {
            "access_key": "abc",
            "secret_key": "secret",
            "host_base": "s3.mlcloud.uni-tuebingen.de",
            "use_https": "true",
        },
    )
    monkeypatch.setattr(app_module.boto3, "client", fake_boto3_client)

    app_module.create_s3_client(
        region=None,
        endpoint_url="http://192.168.213.99",
    )

    assert captured["kwargs"]["endpoint_url"] == "http://192.168.213.99"


def test_merge_experiment_config_resolves_interpolations(monkeypatch: pytest.MonkeyPatch) -> None:
    files = app_module.ExperimentConfigFiles(
        experiment_id="runs/exp-1",
        config_key="runs/exp-1/.hydra/config.yaml",
        hydra_key="runs/exp-1/.hydra/hydra.yaml",
        overrides_key="runs/exp-1/.hydra/overrides.yaml",
    )

    yaml_by_key = {
        files.config_key: {
            "dataset": "cifar10",
            "paths": {"root": "/data/${dataset}"},
            "trainer": {"epochs": 5},
        },
        files.hydra_key: {"runtime": {"output_dir": "outputs/${dataset}"}},
        files.overrides_key: ["trainer.epochs=12"],
    }

    def fake_read_yaml(_s3_client: Any, bucket: str, key: str) -> Any:
        _ = bucket
        return yaml_by_key[key]

    monkeypatch.setattr(app_module, "read_yaml_from_s3", fake_read_yaml)

    flat = app_module.merge_experiment_config(s3_client=object(), bucket="berens0", files=files)

    assert flat["dataset"] == "cifar10"
    assert flat["paths.root"] == "/data/cifar10"
    assert flat["trainer.epochs"] == 12
    assert flat["hydra.runtime.output_dir"] == "outputs/cifar10"


def test_merge_experiment_config_handles_hydra_yaml_with_root_hydra(monkeypatch: pytest.MonkeyPatch) -> None:
    files = app_module.ExperimentConfigFiles(
        experiment_id="runs/exp-1",
        config_key="runs/exp-1/config/config.yaml",
        hydra_key="runs/exp-1/config/hydra.yaml",
        overrides_key="runs/exp-1/config/overrides.yaml",
    )

    yaml_by_key = {
        files.config_key: {"run_name": "run_${now:%Y}"},
        files.hydra_key: {"hydra": {"runtime": {"output_dir": "outputs/${run_name}"}}},
        files.overrides_key: [],
    }

    def fake_read_yaml(_s3_client: Any, bucket: str, key: str) -> Any:
        _ = bucket
        return yaml_by_key[key]

    monkeypatch.setattr(app_module, "read_yaml_from_s3", fake_read_yaml)

    flat = app_module.merge_experiment_config(s3_client=object(), bucket="berens0", files=files)

    assert flat["run_name"] == "exp-1"
    assert flat["hydra.runtime.output_dir"] == "outputs/exp-1"


def test_api_filter_returns_matching_indexes_in_requested_order() -> None:
    job_id = "job-filter-ordered"
    with app_module.JOBS_LOCK:
        app_module.JOBS[job_id] = {
            "status": "done",
            "rows": [
                {"experiment_id": "run-1", "__s3_path": "runs/exp-1", "score": "0.81", "notes": ""},
                {"experiment_id": "run-2", "__s3_path": "runs/exp-2", "score": "0.95", "notes": "kept"},
                {"experiment_id": "run-3", "__s3_path": "runs/exp-3", "score": "1.20", "notes": ""},
            ],
        }

    try:
        client = app_module.app.test_client()
        response = client.post(
            "/api/filter",
            json={
                "job_id": job_id,
                "filters": [
                    {"column": "score", "operator": "gt", "value": "0.9"},
                    {"column": "notes", "operator": "missing"},
                ],
                "ordered_paths": ["runs/exp-3", "runs/exp-2", "runs/exp-1"],
            },
        )

        assert response.status_code == 200
        assert response.get_json() == {"matching_indexes": [0]}
    finally:
        with app_module.JOBS_LOCK:
            app_module.JOBS.pop(job_id, None)


def test_api_filter_rejects_invalid_filter_specs() -> None:
    job_id = "job-filter-invalid"
    with app_module.JOBS_LOCK:
        app_module.JOBS[job_id] = {
            "status": "done",
            "rows": [{"experiment_id": "run-1", "__s3_path": "runs/exp-1", "score": "0.81"}],
        }

    try:
        client = app_module.app.test_client()
        response = client.post(
            "/api/filter",
            json={
                "job_id": job_id,
                "filters": [{"column": "score", "operator": "sideways", "value": "1"}],
            },
        )

        assert response.status_code == 400
        assert response.get_json() == {"error": "Unsupported filter operator: sideways"}
    finally:
        with app_module.JOBS_LOCK:
            app_module.JOBS.pop(job_id, None)


def test_extract_git_hash_value_marks_dirty_diff(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_read_json(_s3_client: Any, bucket: str, key: str) -> Any:
        _ = (bucket, key)
        return {"git": {"hash": "abc123", "diff": "changed line"}}

    monkeypatch.setattr(app_module, "read_json_from_s3", fake_read_json)

    value = app_module.extract_git_hash_value(object(), "berens0", "runs/exp-1")
    assert value == "abc123!"


def test_extract_git_hash_value_reads_top_level_git_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_read_json(_s3_client: Any, bucket: str, key: str) -> Any:
        _ = bucket
        assert key == "runs/exp-1/train_dir/default_experiment/config.json"
        return {"git_hash": "440d351d2a81e97726f821bc86ba6b8c470d9d2c"}

    monkeypatch.setattr(app_module, "read_json_from_s3", fake_read_json)

    value = app_module.extract_git_hash_value(object(), "berens0", "runs/exp-1")
    assert value == "440d351d2a81e97726f821bc86ba6b8c470d9d2c"


def test_extract_git_hash_value_returns_plain_hash_when_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_read_json(_s3_client: Any, bucket: str, key: str) -> Any:
        _ = (bucket, key)
        return {"git": {"hash": "def456", "diff": ""}}

    monkeypatch.setattr(app_module, "read_json_from_s3", fake_read_json)

    value = app_module.extract_git_hash_value(object(), "berens0", "runs/exp-1")
    assert value == "def456"


def test_extract_git_hash_value_falls_back_to_root_config_json(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_read_json(_s3_client: Any, bucket: str, key: str) -> Any:
        _ = bucket
        calls.append(key)
        if key in {
            "runs/exp-1/train_dir/default_experiment/config.json",
            "runs/exp-1/default_experiment/config.json",
        }:
            raise botocore.exceptions.ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        if key == "runs/exp-1/config.json":
            return {"git_hash": "xyz789", "git_diff": "dirty"}
        return {}

    monkeypatch.setattr(app_module, "read_json_from_s3", fake_read_json)

    value = app_module.extract_git_hash_value(object(), "berens0", "runs/exp-1")
    assert value == "xyz789!"
    assert calls == [
        "runs/exp-1/train_dir/default_experiment/config.json",
        "runs/exp-1/default_experiment/config.json",
        "runs/exp-1/config.json",
    ]


def test_extract_last_analysis_step_returns_max_epoch() -> None:
    class FakePaginator:
        def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
            _ = kwargs
            return [
                {
                    "Contents": [
                        {"Key": "runs/exp-1/data/analyses/receptive_fields_epoch_10.npz"},
                        {"Key": "runs/exp-1/data/analyses/receptive_fields_epoch_100.npz"},
                        {"Key": "runs/exp-1/data/analyses/other_file.txt"},
                    ]
                }
            ]

    class FakeS3Client:
        def get_paginator(self, operation_name: str) -> FakePaginator:
            assert operation_name == "list_objects_v2"
            return FakePaginator()

    value = app_module.extract_last_analysis_step(FakeS3Client(), "berens0", "runs/exp-1")
    assert value == "100"


def test_load_experiment_rows_uses_short_path_as_run_name(monkeypatch: pytest.MonkeyPatch) -> None:
    files = app_module.ExperimentConfigFiles(
        experiment_id="runs/group-a/run_2025-11-24-14-54-53-497055",
        config_key="runs/group-a/run_2025-11-24-14-54-53-497055/.hydra/config.yaml",
    )

    monkeypatch.setattr(app_module, "create_s3_client", lambda region=None, endpoint_url=None: object())
    monkeypatch.setattr(app_module, "iter_yaml_keys", lambda s3_client, bucket, prefix="": [files.config_key])
    monkeypatch.setattr(
        app_module,
        "collect_experiment_files",
        lambda keys: {files.experiment_id: files},
    )
    monkeypatch.setattr(
        app_module,
        "merge_experiment_config",
        lambda s3_client, bucket, files: {"run_name": "run_${now:%Y}", "trainer.epochs": 5},
    )
    monkeypatch.setattr(app_module, "extract_git_hash_value", lambda s3_client, bucket, experiment_id: "")
    monkeypatch.setattr(app_module, "extract_last_analysis_step", lambda s3_client, bucket, experiment_id: "")

    rows, columns, error = app_module.load_experiment_rows(bucket="berens0", prefix="runs/")

    assert error is None
    assert columns[0] == "run_name"
    assert "experiment_id" not in columns
    assert rows[0]["run_name"] == "run_2025-11-24-14-54-53-497055"
    assert rows[0]["trainer.epochs"] == "5"
