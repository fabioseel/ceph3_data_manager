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


def test_merge_experiment_config_ignores_hydra_choice_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    files = app_module.ExperimentConfigFiles(
        experiment_id="runs/exp-1",
        config_key="runs/exp-1/config/config.yaml",
        hydra_key="runs/exp-1/config/hydra.yaml",
        overrides_key="runs/exp-1/config/overrides.yaml",
    )

    yaml_by_key = {
        files.config_key: {
            "optimizer": {
                "num_epochs": 200,
                "optimizer": {"_target_": "torch.optim.Adam", "lr": 0.001},
            },
            "brain": {"circuits": {"retina": {"num_layers": 2}}},
            "logging": {"wandb_project": "baseline"},
        },
        files.hydra_key: {
            "hydra": {
                "runtime": {
                    "choices": {
                        "optimizer": "eff-brain-rand-retina",
                        "brain": "eff-brain-rl",
                        "experiment": "eff-coding",
                    }
                }
            }
        },
        files.overrides_key: [
            "optimizer=eff-brain-rand-retina",
            "brain=eff-brain-rl",
            "+experiment=eff-coding",
            "logging.wandb_project=override-project",
        ],
    }

    def fake_read_yaml(_s3_client: Any, bucket: str, key: str) -> Any:
        _ = bucket
        return yaml_by_key[key]

    monkeypatch.setattr(app_module, "read_yaml_from_s3", fake_read_yaml)

    flat = app_module.merge_experiment_config(s3_client=object(), bucket="berens0", files=files)

    assert flat["optimizer.num_epochs"] == 200
    assert flat["optimizer.optimizer._target_"] == "torch.optim.Adam"
    assert flat["brain.circuits.retina.num_layers"] == 2
    assert flat["logging.wandb_project"] == "override-project"


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


def test_api_filter_supports_or_combination() -> None:
    job_id = "job-filter-or"
    with app_module.JOBS_LOCK:
        app_module.JOBS[job_id] = {
            "status": "done",
            "rows": [
                {"experiment_id": "run-1", "__s3_path": "runs/exp-1", "score": "0.81", "notes": ""},
                {"experiment_id": "run-2", "__s3_path": "runs/exp-2", "score": "0.95", "notes": "kept"},
                {"experiment_id": "run-3", "__s3_path": "runs/exp-3", "score": "0.40", "notes": ""},
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
                "combine_with": "or",
            },
        )

        assert response.status_code == 200
        assert response.get_json() == {"matching_indexes": [0, 1, 2]}
    finally:
        with app_module.JOBS_LOCK:
            app_module.JOBS.pop(job_id, None)


def test_api_filter_rejects_invalid_filter_logic() -> None:
    job_id = "job-filter-invalid-logic"
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
                "filters": [{"column": "score", "operator": "gt", "value": "0.9"}],
                "combine_with": "xor",
            },
        )

        assert response.status_code == 400
        assert response.get_json() == {"error": "Unsupported filter logic: xor"}
    finally:
        with app_module.JOBS_LOCK:
            app_module.JOBS.pop(job_id, None)


def test_api_filter_supports_nested_filter_groups() -> None:
    """Test API with nested filter group structure."""
    job_id = "job-filter-nested"
    with app_module.JOBS_LOCK:
        app_module.JOBS[job_id] = {
            "status": "done",
            "rows": [
                {"experiment_id": "run-1", "__s3_path": "runs/exp-1", "score": "0.81", "tag": "baseline", "epoch": "10"},
                {"experiment_id": "run-2", "__s3_path": "runs/exp-2", "score": "0.95", "tag": "aug", "epoch": "25"},
                {"experiment_id": "run-3", "__s3_path": "runs/exp-3", "score": "1.20", "tag": "baseline", "epoch": "40"},
            ],
        }

    try:
        client = app_module.app.test_client()
        # Create nested structure: (tag=baseline AND score>0.9) OR (epoch>30)
        response = client.post(
            "/api/filter",
            json={
                "job_id": job_id,
                "filter_group": {
                    "logic": "or",
                    "items": [
                        {
                            "logic": "and",
                            "items": [
                                {"column": "tag", "operator": "contains", "value": "baseline"},
                                {"column": "score", "operator": "gt", "value": "0.9"},
                            ],
                        },
                        {"column": "epoch", "operator": "gt", "value": "30"},
                    ],
                },
                "ordered_paths": ["runs/exp-1", "runs/exp-2", "runs/exp-3"],
            },
        )

        assert response.status_code == 200
        # Matches: exp-3 (baseline AND score > 0.9) and exp-3 also matches (epoch > 30)
        # exp-2 has epoch=25 (not > 30), so only exp-3 matches
        assert sorted(response.get_json()["matching_indexes"]) == [2]
    finally:
        with app_module.JOBS_LOCK:
            app_module.JOBS.pop(job_id, None)


def test_api_filter_in_other_column_use_preceding_context() -> None:
    """use_visible_rows_only uses preceding AND filter results as context (not visible_row_indexes)."""
    job_id = "job-filter-in-other-column"
    with app_module.JOBS_LOCK:
        app_module.JOBS[job_id] = {
            "status": "done",
            "rows": [
                {"__s3_path": "runs/exp-1", "name": "A", "ref": "B"},
                {"__s3_path": "runs/exp-2", "name": "B", "ref": "C"},
                {"__s3_path": "runs/exp-3", "name": "C", "ref": "A"},
                {"__s3_path": "runs/exp-4", "name": "D", "ref": "D"},
            ],
        }

    try:
        client = app_module.app.test_client()
        response = client.post(
            "/api/filter",
            json={
                "job_id": job_id,
                "filter_group": {
                    "logic": "and",
                    "items": [
                        {"column": "name", "operator": "is_one_of", "value": ["A", "B"]},
                        {"column": "name", "operator": "in_other_column", "value": "ref", "use_visible_rows_only": True},
                    ],
                },
                "visible_row_indexes": [3],  # ignored for AND groups
            },
        )

        assert response.status_code == 200
        # context = rows passing first filter (A, B); lookup from their ref = {B, C}
        # applied to current_result [A, B]: A∈{B,C}? No  B∈{B,C}? Yes → [1]
        assert response.get_json() == {"matching_indexes": [1]}
    finally:
        with app_module.JOBS_LOCK:
            app_module.JOBS.pop(job_id, None)


def test_api_filter_in_other_column_matches_substrings() -> None:
    job_id = "job-filter-in-other-column-substring"
    with app_module.JOBS_LOCK:
        app_module.JOBS[job_id] = {
            "status": "done",
            "rows": [
                {"experiment_id": "run-1", "__s3_path": "runs/exp-1", "tag": "base", "peer": "baseline"},
                {"experiment_id": "run-2", "__s3_path": "runs/exp-2", "tag": "line", "peer": "baseline"},
                {"experiment_id": "run-3", "__s3_path": "runs/exp-3", "tag": "zzz", "peer": "baseline"},
            ],
        }

    try:
        client = app_module.app.test_client()
        response = client.post(
            "/api/filter",
            json={
                "job_id": job_id,
                "filter_group": {
                    "logic": "and",
                    "items": [
                        {
                            "column": "tag",
                            "operator": "partially_in_other_column",
                            "value": "peer",
                        }
                    ],
                },
            },
        )

        assert response.status_code == 200
        assert response.get_json() == {"matching_indexes": [0, 1]}
    finally:
        with app_module.JOBS_LOCK:
            app_module.JOBS.pop(job_id, None)


def test_api_filter_in_other_column_does_not_match_substrings() -> None:
    job_id = "job-filter-in-other-column-exact"
    with app_module.JOBS_LOCK:
        app_module.JOBS[job_id] = {
            "status": "done",
            "rows": [
                {"experiment_id": "run-1", "__s3_path": "runs/exp-1", "tag": "base", "peer": "baseline"},
                {"experiment_id": "run-2", "__s3_path": "runs/exp-2", "tag": "line", "peer": "baseline"},
                {"experiment_id": "run-3", "__s3_path": "runs/exp-3", "tag": "zzz", "peer": "baseline"},
            ],
        }

    try:
        client = app_module.app.test_client()
        response = client.post(
            "/api/filter",
            json={
                "job_id": job_id,
                "filter_group": {
                    "logic": "and",
                    "items": [
                        {
                            "column": "tag",
                            "operator": "in_other_column",
                            "value": "peer",
                        }
                    ],
                },
            },
        )

        assert response.status_code == 200
        assert response.get_json() == {"matching_indexes": []}
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

    rows, columns, error = app_module.load_experiment_rows(bucket="berens0", prefixes=["runs/"])

    assert error is None
    assert columns[0] == "run_name"
    assert "experiment_id" not in columns
    assert rows[0]["run_name"] == "run_2025-11-24-14-54-53-497055"
    assert rows[0]["trainer.epochs"] == "5"


def test_load_experiment_rows_incremental_reuses_cached_and_fetches_only_new(monkeypatch: pytest.MonkeyPatch) -> None:
    exp1 = app_module.ExperimentConfigFiles(experiment_id="runs/exp-1", config_key="runs/exp-1/config/config.yaml")
    exp2 = app_module.ExperimentConfigFiles(experiment_id="runs/exp-2", config_key="runs/exp-2/config/config.yaml")

    monkeypatch.setattr(app_module, "_discover_experiments", lambda **_: {"runs/exp-1": exp1, "runs/exp-2": exp2})

    def fake_load_experiment_rows(
        bucket: str,
        prefixes: list[str] | None = None,
        region: str | None = None,
        endpoint_url: str | None = None,
        progress_callback: Any | None = None,
        should_cancel: Any | None = None,
        experiments: dict[str, app_module.ExperimentConfigFiles] | None = None,
        initial_processed: int = 0,
        total_override: int | None = None,
    ) -> tuple[list[dict[str, str]], list[str], str | None]:
        _ = (bucket, prefixes, region, endpoint_url, progress_callback, should_cancel)
        assert experiments is not None
        assert sorted(experiments.keys()) == ["runs/exp-2"]
        assert initial_processed == 1
        assert total_override == 2
        return [
            {
                "run_name": "exp-2",
                "__s3_path": "runs/exp-2",
                "trainer.epochs": "20",
            }
        ], ["run_name", "trainer.epochs"], None

    monkeypatch.setattr(app_module, "load_experiment_rows", fake_load_experiment_rows)

    cached_rows = [
        {
            "run_name": "exp-1",
            "__s3_path": "runs/exp-1",
            "trainer.epochs": "10",
        }
    ]

    rows, columns, error = app_module.load_experiment_rows_incremental(
        bucket="berens0",
        cached_rows=cached_rows,
        prefixes=["runs/"],
    )

    assert error is None
    assert [row["__s3_path"] for row in rows] == ["runs/exp-1", "runs/exp-2"]
    assert rows[0]["trainer.epochs"] == "10"
    assert rows[1]["trainer.epochs"] == "20"
    assert columns[0] == "run_name"
    assert "trainer.epochs" in columns


def test_run_load_job_uses_incremental_loading_when_cache_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    job_id = "job-incremental-cache"
    with app_module.JOBS_LOCK:
        app_module.JOBS[job_id] = {
            "status": "running",
            "phase": "starting",
            "processed": 0,
            "total": 0,
            "error": None,
            "rows": [],
            "columns": [],
            "cancel_requested": False,
            "cached_at": None,
        }

    monkeypatch.setattr(
        app_module,
        "_read_cache",
        lambda bucket, prefixes: ([{"run_name": "exp-1", "__s3_path": "runs/exp-1"}], ["run_name"], "2026-01-01T00:00:00+00:00"),
    )

    def fake_incremental(**kwargs: Any) -> tuple[list[dict[str, str]], list[str], str | None]:
        _ = kwargs
        return [{"run_name": "exp-1", "__s3_path": "runs/exp-1", "trainer.epochs": "10"}], ["run_name", "trainer.epochs"], None

    monkeypatch.setattr(app_module, "load_experiment_rows_incremental", fake_incremental)

    def fail_full_load(**kwargs: Any) -> tuple[list[dict[str, str]], list[str], str | None]:
        raise AssertionError("full load should not be used when cache exists and refresh is false")

    monkeypatch.setattr(app_module, "load_experiment_rows", fail_full_load)

    wrote: dict[str, Any] = {}

    def fake_write_cache(bucket: str, prefixes: list[str], rows: list, columns: list) -> None:
        wrote["bucket"] = bucket
        wrote["prefixes"] = prefixes
        wrote["rows"] = rows
        wrote["columns"] = columns

    monkeypatch.setattr(app_module, "_write_cache", fake_write_cache)

    try:
        app_module.run_load_job(
            job_id=job_id,
            bucket="berens0",
            prefixes=["runs/"],
            region=None,
            endpoint_url=None,
            force_refresh=False,
        )

        state = app_module.get_job_state(job_id)
        assert state is not None
        assert state["status"] == "done"
        assert len(state["rows"]) == 1
        assert wrote["bucket"] == "berens0"
        assert wrote["prefixes"] == ["runs/"]
        assert wrote["columns"] == ["run_name", "trainer.epochs"]
    finally:
        with app_module.JOBS_LOCK:
            app_module.JOBS.pop(job_id, None)
