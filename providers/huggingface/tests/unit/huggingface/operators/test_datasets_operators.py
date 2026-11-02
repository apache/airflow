# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Regression coverage for durable load/map/publish behavior."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from datasets import ClassLabel, Dataset, DatasetDict, Features, Image, Value
from huggingface_hub import CommitInfo, GitCommitInfo

from airflow.providers.huggingface.operators.datasets import (
    LoadDatasetOperator,
    MapDatasetOperator,
    PushToHubOperator,
    StreamDatasetOperator,
    _is_hub_repo,
    _publication_marker,
    _load_artifact,
    _save,
)


def add_one(example):
    return {"value": example["value"] + 1}


def commit_info(repo_id="org/data", oid="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", message="publish"):
    url = f"https://huggingface.co/datasets/{repo_id}/commit/{oid}"
    return CommitInfo(commit_url=url, commit_message=message, commit_description="", oid=oid)


def git_commit_info(commit_id, title, message=""):
    return GitCommitInfo(
        commit_id=commit_id,
        authors=["unit-test"],
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        title=title,
        message=message,
        formatted_title=None,
        formatted_message=None,
    )



@pytest.fixture
def hub_dependencies(monkeypatch):
    """Use a real Airflow Connection while mocking only Hub and Datasets APIs."""
    monkeypatch.setenv(
        "AIRFLOW_CONN_HUGGINGFACE_TEST",
        '{"conn_type":"huggingface","password":"operator-test-token"}',
    )
    with (
        patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as hf_api,
        patch("datasets.Dataset.push_to_hub", autospec=True) as push_to_hub,
    ):
        hf_api.return_value.list_repo_commits.return_value = []
        yield hf_api, push_to_hub


def test_load_pins_and_records_resolved_revision_and_honors_download_mode(tmp_path, monkeypatch):
    output = (tmp_path / "loaded.json").as_uri()
    dataset = Dataset.from_dict({"value": [1, 2]})
    monkeypatch.setenv("AIRFLOW_CONN_HUGGINGFACE_TEST", '{"conn_type":"huggingface","password":"operator-test-token"}')
    with (
        patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as hf_api,
        patch("airflow.providers.huggingface.hooks.huggingface.load_dataset", autospec=True) as load_dataset,
    ):
        hf_api.return_value.dataset_info.return_value.sha = "resolved-sha"
        load_dataset.return_value = dataset
        operator = LoadDatasetOperator(
            task_id="load", path="org/data", revision="release-tag", output_uri=output,
            download_mode="force_redownload", cache_dir=str(tmp_path / "cache"),
            huggingface_conn_id="huggingface_test",
        )
        reference = operator.execute({})
    hf_api.return_value.dataset_info.assert_called_once_with("org/data", revision="release-tag")
    load_dataset.assert_called_once_with(
        "org/data", revision="resolved-sha", download_mode="force_redownload", cache_dir=str(tmp_path / "cache"),
        token="operator-test-token",
    )
    assert reference["revision"] == "resolved-sha"
    assert _load_artifact(reference).num_rows == 2


def test_non_hub_load_does_not_resolve_hub_revision(tmp_path):
    with patch("airflow.providers.huggingface.hooks.huggingface.load_dataset", autospec=True) as load_dataset:
        load_dataset.return_value = Dataset.from_dict({"x": [1]})
        operator = LoadDatasetOperator(task_id="csv", path="csv", output_uri=(tmp_path / "csv.json").as_uri(),
                                       huggingface_conn_id=None)
        result = operator.execute({})
    load_dataset.assert_called_once_with("csv", download_mode="reuse_dataset_if_exists")
    assert result["revision"] is None


def test_stream_operator_lazily_transforms_filters_and_publishes_with_asset_event():
    transformed = lambda row, increment: {"value": row["value"] + increment}
    predicate = lambda row: row["value"] > 1
    stream = MagicMock()
    stream.map.return_value = stream
    stream.filter.return_value = stream
    commit = commit_info("org/streamed", message="Stream dataset from Airflow")
    with patch("airflow.providers.huggingface.operators.datasets.HuggingFaceHook", autospec=True) as hook_type:
        hook = hook_type.return_value
        hook.get_dataset_info.return_value.sha = "pinned-stream-sha"
        hook.load_dataset.return_value = stream
        hook.find_commit_by_marker.return_value = None
        hook.push_to_hub.return_value = commit
        operator = StreamDatasetOperator(
            task_id="stream",
            path="org/source",
            split="train",
            revision="main",
            repo_id="org/streamed",
            config="clean",
            private=True,
            transform=transformed,
            transform_kwargs={"increment": 1},
            predicate=predicate,
            huggingface_conn_id=None,
        )
        event = SimpleNamespace(extra=None)
        result = operator.execute({"run_id": "scheduled__2026-10-31", "outlet_events": {operator.published_asset: event}})

    hook.get_dataset_info.assert_called_once_with("org/source", "main")
    hook.load_dataset.assert_called_once_with(
        "org/source", streaming=True, split="train", revision="pinned-stream-sha"
    )
    hook.push_to_hub.assert_called_once()
    stream.map.assert_called_once_with(transformed, fn_kwargs={"increment": 1})
    stream.filter.assert_called_once_with(predicate)
    assert hook.push_to_hub.call_args.args == (stream, "org/streamed")
    push_kwargs = hook.push_to_hub.call_args.kwargs
    assert push_kwargs["config_name"] == "clean"
    assert push_kwargs["private"] is True
    assert "huggingface-stream-publish:" in push_kwargs["commit_message"]
    assert result["source_revision"] == "pinned-stream-sha"
    assert result["commit_sha"] == commit.oid
    assert result["streaming"] is True
    assert event.extra["commit_sha"] == commit.oid


def test_stream_operator_recovers_prior_commit_without_reloading_stream():
    commit = commit_info("org/streamed")
    with patch("airflow.providers.huggingface.operators.datasets.HuggingFaceHook", autospec=True) as hook_type:
        hook = hook_type.return_value
        hook.find_commit_by_marker.return_value = commit
        operator = StreamDatasetOperator(
            task_id="stream",
            path="csv",
            repo_id="org/streamed",
            huggingface_conn_id=None,
        )
        result = operator.execute({"run_id": "scheduled__2026-10-31"})

    hook.load_dataset.assert_not_called()
    hook.push_to_hub.assert_not_called()
    assert result["commit_sha"] == commit.oid


def test_parquet_manifest_preserves_order_features_and_dataset_splits(tmp_path):
    features = Features({"z_first": Value("int64"), "a_second": Value("string"),
                         "kind": ClassLabel(names=["first", "second"])})
    source = DatasetDict({
        "train": Dataset.from_dict({"z_first": [1], "a_second": ["one"], "kind": [0]}, features=features),
        "validation": Dataset.from_dict({"z_first": [2], "a_second": ["two"], "kind": [1]}, features=features),
    })
    reference = _save(source, (tmp_path / "splits.json").as_uri(), {"revision": "abc"})
    restored = _load_artifact(reference)
    assert list(restored) == ["train", "validation"]
    assert restored["train"].column_names == ["z_first", "a_second", "kind"]
    assert restored["validation"].features == features
    assert restored["train"].features["kind"].int2str(0) == "first"
    assert reference["format"] == "parquet"
    assert reference["fingerprint"]
    assert reference["splits"]["train"]["rows"] == 1


def test_single_dataset_split_name_survives_artifact_round_trip(tmp_path):
    source = Dataset.from_dict({"value": [1]}, split="validation")
    reference = _save(source, (tmp_path / "single-split.json").as_uri())
    restored = _load_artifact(reference)
    assert restored.split == "validation"


def test_parquet_rejects_media_feature_types(tmp_path):
    dataset = Dataset.from_dict({"image": [None]}, features=Features({"image": Image()}))
    with pytest.raises(ValueError, match="unsupported Parquet feature type"):
        _save(dataset, (tmp_path / "image.json").as_uri())


def test_map_uses_parse_time_callable_and_persists_reference(tmp_path):
    source = _save(Dataset.from_dict({"value": [1, 2]}), (tmp_path / "input.json").as_uri(),
                   {"repository_id": "org/data", "revision": "source-sha"})
    source.update(repository_id="org/data", revision="source-sha")
    operator = MapDatasetOperator(task_id="map", input_uri=source,
                                  output_uri=(tmp_path / "mapped.json").as_uri(), transform=add_one)
    reference = operator.execute({})
    assert _load_artifact(reference)["value"] == [2, 3]
    assert reference["revision"] == "source-sha"
    assert "transform" not in operator.template_fields
    with pytest.raises(TypeError, match="callable"):
        MapDatasetOperator(task_id="bad", input_uri=source, output_uri="file:///tmp/out.json", transform="x:y")


def test_map_transform_exception_propagates(tmp_path):
    source = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "input.json").as_uri())
    error = LookupError("transform failed")
    def fail(example):
        raise error
    operator = MapDatasetOperator(task_id="map_fail", input_uri=source,
                                  output_uri=(tmp_path / "mapped.json").as_uri(), transform=fail)
    with pytest.raises(LookupError) as raised:
        operator.execute({})
    assert raised.value is error


def test_fingerprint_and_split_checksum_mismatches_are_rejected(tmp_path):
    reference = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "input.json").as_uri())
    changed_reference = dict(reference, fingerprint="wrong")
    with pytest.raises(ValueError, match="artifact fingerprint"):
        _load_artifact(changed_reference)
    split_path = tmp_path / "input.json.split-00000.parquet"
    split_path.write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="split fingerprint"):
        _load_artifact(reference)


def test_push_returns_commit_info_and_emits_matching_asset_event(tmp_path, hub_dependencies):
    hf_api, push_to_hub = hub_dependencies
    reference = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "input.json").as_uri())
    commit = commit_info()
    push_to_hub.return_value = commit
    operator = PushToHubOperator(task_id="publish", input_uri=reference, repo_id="org/data",
                                 commit_message="publish source", huggingface_conn_id="huggingface_test")
    event = type("Event", (), {"extra": None})()
    class Events:
        def __getitem__(self, asset):
            assert asset.uri == "hf://datasets/org/data"
            return event
    result = operator.execute({"outlet_events": Events()})
    push_to_hub.assert_called_once()
    kwargs = push_to_hub.call_args.kwargs
    assert reference["fingerprint"] in kwargs["commit_message"]
    hf_api.return_value.list_repo_commits.assert_called_once_with(
        "org/data", repo_type="dataset", revision="main"
    )
    assert result["commit_sha"] == commit.oid
    assert result["commit_url"] == commit.commit_url
    assert event.extra["commit_sha"] == result["commit_sha"]
    assert event.extra["commit_url"] == result["commit_url"]


def test_push_failure_propagates_and_emits_no_event(tmp_path, hub_dependencies):
    _, push_to_hub = hub_dependencies
    reference = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "input.json").as_uri())
    error = ValueError("publication denied")
    push_to_hub.side_effect = error
    operator = PushToHubOperator(task_id="failed", input_uri=reference, repo_id="org/data",
                                 huggingface_conn_id="huggingface_test")
    event = type("Event", (), {"extra": None})()
    class Events:
        def __getitem__(self, asset): return event
    with pytest.raises(ValueError) as raised:
        operator.execute({"outlet_events": Events()})
    assert raised.value is error
    assert event.extra is None


def test_push_requires_reference_and_validates_fingerprint(tmp_path):
    reference = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "input.json").as_uri())
    operator = PushToHubOperator(task_id="publish", input_uri=reference["uri"], repo_id="org/data")
    with pytest.raises(ValueError, match="requires an artifact reference"):
        operator.execute({})
    reference["fingerprint"] = "bad"
    operator = PushToHubOperator(task_id="publish_bad", input_uri=reference, repo_id="org/data", durable=False)
    with pytest.raises(ValueError, match="artifact fingerprint"):
        operator.execute({})


def test_airflow_30_path_without_state_store_and_retry_with_store(tmp_path, hub_dependencies):
    hf_api, _ = hub_dependencies
    reference = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "input.json").as_uri())
    target_marker = _publication_marker(
        "target", {"repo_id": "org/data", "config_name": "default", "revision": "main", "create_pr": False}
    )
    marker = f"airflow-artifact-sha256:{reference['fingerprint']}:{target_marker}"
    commit = git_commit_info("a" * 40, f"Publish [{marker}]")
    hf_api.return_value.list_repo_commits.return_value = [commit]
    operator = PushToHubOperator(task_id="legacy", input_uri=reference, repo_id="org/data",
                                 huggingface_conn_id="huggingface_test")
    result = operator.execute({})
    assert result["commit_sha"] == "a" * 40
    store = type("Store", (), {"get": lambda self, key: result,
                                "set": lambda self, key, value: None})()
    event = type("Event", (), {"extra": None})()
    class Events:
        def __getitem__(self, asset): return event
    assert operator.execute({"task_state_store": store, "outlet_events": Events()}) == result
    assert event.extra["commit_sha"] == result["commit_sha"]


def test_push_reuses_hub_commit_before_loading_missing_artifact(tmp_path, hub_dependencies):
    hf_api, push_to_hub = hub_dependencies
    reference = {
        "uri": (tmp_path / "deleted.json").as_uri(),
        "fingerprint": "f" * 64,
        "repository_id": "org/source",
        "revision": "source-sha",
    }
    target_marker = _publication_marker(
        "target", {"repo_id": "org/data", "config_name": "default", "revision": "main", "create_pr": False}
    )
    marker = f"airflow-artifact-sha256:{reference['fingerprint']}:{target_marker}"
    commit = git_commit_info("b" * 40, f"Publish [{marker}]")
    hf_api.return_value.list_repo_commits.return_value = [commit]
    operator = PushToHubOperator(
        task_id="recover_missing", input_uri=reference, repo_id="org/data", huggingface_conn_id="huggingface_test"
    )
    with patch("airflow.providers.huggingface.operators.datasets._load_artifact") as load:
        result = operator.execute({})
    load.assert_not_called()
    push_to_hub.assert_not_called()
    assert result["commit_sha"] == "b" * 40


def test_state_store_write_failure_retry_does_not_push_twice(tmp_path, hub_dependencies):
    hf_api, push_to_hub = hub_dependencies
    reference = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "retry_after_push.json").as_uri())
    target_marker = _publication_marker(
        "target", {"repo_id": "org/data", "config_name": "default", "revision": "main", "create_pr": False}
    )
    marker = f"airflow-artifact-sha256:{reference['fingerprint']}:{target_marker}"
    commit = git_commit_info("a" * 40, f"Publish [{marker}]")
    hf_api.return_value.list_repo_commits.side_effect = [[], [commit]]
    push_to_hub.return_value = commit
    operator = PushToHubOperator(task_id="retry_after_push", input_uri=reference, repo_id="org/data",
                                 huggingface_conn_id="huggingface_test")
    class FailingStore:
        def get(self, key): return None
        def set(self, key, value): raise RuntimeError("worker failed after Hub commit")
    with pytest.raises(RuntimeError, match="worker failed"):
        operator.execute({"task_state_store": FailingStore()})
    class Store:
        def get(self, key): return None
        def set(self, key, value): pass
    retry_result = operator.execute({"task_state_store": Store()})
    push_to_hub.assert_called_once()
    assert retry_result["commit_sha"] == "a" * 40


def test_push_does_not_treat_intermediate_upload_part_as_published(tmp_path, hub_dependencies):
    hf_api, push_to_hub = hub_dependencies
    reference = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "partial.json").as_uri())
    target_marker = _publication_marker(
        "target", {"repo_id": "org/data", "config_name": "default", "revision": "main", "create_pr": False}
    )
    marker = f"airflow-artifact-sha256:{reference['fingerprint']}:{target_marker}"
    hf_api.return_value.list_repo_commits.return_value = [
        git_commit_info("a" * 40, f"Publish [{marker}] (part 1-of-2)")
    ]
    pushed_commit = commit_info(oid="b" * 40)
    push_to_hub.return_value = pushed_commit

    operator = PushToHubOperator(
        task_id="partial_upload", input_uri=reference, repo_id="org/data", huggingface_conn_id="huggingface_test"
    )
    result = operator.execute({})

    push_to_hub.assert_called_once()
    assert result["commit_sha"] == pushed_commit.oid


def test_push_idempotency_marker_is_scoped_to_config_revision_and_pr_target(tmp_path):
    reference = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "targets.json").as_uri())
    commits = [commit_info(oid=f"{index:040x}") for index in range(1, 4)]
    with patch("airflow.providers.huggingface.operators.datasets.HuggingFaceHook", autospec=True) as hook_type:
        hook = hook_type.return_value
        hook.find_publication_commit.return_value = None
        hook.push_to_hub.side_effect = commits
        configs = [
            ("config-a", {"revision": "branch-a"}),
            ("config-b", {"revision": "branch-a"}),
            ("config-b", {"revision": "branch-b", "create_pr": True}),
        ]
        for index, (config, push_kwargs) in enumerate(configs):
            operator = PushToHubOperator(
                task_id=f"publish_{index}",
                input_uri=reference,
                repo_id="org/data",
                config=config,
                push_kwargs=push_kwargs,
                huggingface_conn_id=None,
            )
            operator.execute({})

    markers = [call.kwargs["marker"] for call in hook.find_publication_commit.call_args_list]
    assert len(set(markers)) == 3
    assert hook.find_publication_commit.call_args_list[2].kwargs["include_pull_requests"] is True


def test_load_retry_reuses_resolved_revision_from_task_state_store(tmp_path, monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_HUGGINGFACE_TEST", '{"conn_type":"huggingface","password":"operator-test-token"}')
    dataset = Dataset.from_dict({"value": [1]})
    store_data = {}
    class Store:
        def get(self, key): return store_data.get(key)
        def set(self, key, value): store_data[key] = value
    ti = type("TaskInstance", (), {"dag_id": "d", "task_id": "load"})()
    context = {"task_state_store": Store(), "ti": ti}
    with (
        patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api,
        patch("airflow.providers.huggingface.hooks.huggingface.load_dataset", autospec=True) as load_dataset,
    ):
        api.return_value.dataset_info.return_value = type("Info", (), {"sha": "pinned-sha"})()
        load_dataset.return_value = dataset
        operator = LoadDatasetOperator(
            task_id="load", path="org/data", revision="main",
            output_uri=(tmp_path / "first.json").as_uri(),
            huggingface_conn_id="huggingface_test",
        )
        first_result = operator.execute(context)
        operator.output_uri = (tmp_path / "retry.json").as_uri()
        retry_result = operator.execute(context)
    assert first_result["revision"] == retry_result["revision"] == "pinned-sha"
    api.return_value.dataset_info.assert_called_once_with("org/data", revision="main")
    assert [call.kwargs["revision"] for call in load_dataset.call_args_list] == [
        "pinned-sha",
        "pinned-sha",
    ]


def test_load_durable_false_does_not_read_or_write_task_state(tmp_path, monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_HUGGINGFACE_TEST", '{"conn_type":"huggingface","password":"operator-test-token"}')
    with (
        patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api,
        patch("airflow.providers.huggingface.hooks.huggingface.load_dataset", autospec=True) as load_dataset,
    ):
        api.return_value.dataset_info.side_effect = [
            type("Info", (), {"sha": "first-sha"})(),
            type("Info", (), {"sha": "second-sha"})(),
        ]
        load_dataset.return_value = Dataset.from_dict({"value": [1]})
        operator = LoadDatasetOperator(
            task_id="load", path="org/data", revision="main",
            output_uri=(tmp_path / "first.json").as_uri(),
            huggingface_conn_id="huggingface_test", durable=False,
        )
        store = MagicMock()
        context = {"task_state_store": store}
        first = operator.execute(context)
        operator.output_uri = (tmp_path / "second.json").as_uri()
        second = operator.execute(context)
    assert first["revision"] == "first-sha"
    assert second["revision"] == "second-sha"
    store.get.assert_not_called()
    store.set.assert_not_called()
    assert api.return_value.dataset_info.call_count == 2


def test_push_preserves_user_outlets_and_changes_commit_for_new_fingerprint(tmp_path, hub_dependencies):
    hf_api, push_to_hub = hub_dependencies
    reference = _save(Dataset.from_dict({"value": [1]}), (tmp_path / "input.json").as_uri())
    caller_asset = type("Asset", (), {"uri": "custom://caller"})()
    first_commit = commit_info(oid="1" * 40)
    second_commit = commit_info(oid="2" * 40)
    push_to_hub.side_effect = [first_commit, second_commit]
    state_data = {}

    class Store:
        def get(self, key):
            return state_data.get(key)

        def set(self, key, value):
            state_data[key] = value

    operator = PushToHubOperator(task_id="publish", input_uri=reference, repo_id="org/data",
                                 huggingface_conn_id="huggingface_test", outlets=[caller_asset])
    assert caller_asset in operator.outlets
    assert operator.published_asset in operator.outlets
    operator.execute({"task_state_store": Store()})
    changed = _save(Dataset.from_dict({"value": [2]}), (tmp_path / "changed.json").as_uri())
    operator.input_uri = changed
    result = operator.execute({"task_state_store": Store()})
    assert push_to_hub.call_count == 2
    assert result["commit_sha"] == second_commit.oid


def test_push_rejects_templated_repo_id():
    with pytest.raises(ValueError, match="repo_id cannot be templated"):
        PushToHubOperator(task_id="publish", input_uri={}, repo_id="org/{{ params.repo }}")


def test_existing_local_path_is_not_treated_as_hub_repo(tmp_path):
    local_path = tmp_path / "datasets" / "local"
    local_path.mkdir(parents=True)
    assert _is_hub_repo(str(local_path)) is False

    with (
        patch("airflow.providers.huggingface.operators.datasets.HuggingFaceHook", autospec=True) as hook_type,
        patch("airflow.providers.huggingface.operators.datasets._save") as save,
    ):
        hook = hook_type.return_value
        hook.load_dataset.return_value = Dataset.from_dict({"value": [1]})
        save.return_value = {"revision": None, "repository_id": None}
        operator = LoadDatasetOperator(
            task_id="load_local", path=str(local_path), output_uri="file:///tmp/local.json"
        )
        result = operator.execute({})
    hook.get_dataset_info.assert_not_called()
    hook.load_dataset.assert_called_once_with(str(local_path), download_mode="reuse_dataset_if_exists")
    assert result["revision"] is None
    assert result["repository_id"] is None


def test_object_storage_memory_scheme_handoff(monkeypatch):
    """Exercise non-local ObjectStoragePath open semantics without cloud credentials."""
    from fsspec.implementations.memory import MemoryFileSystem
    from airflow.sdk.io import fs as airflow_fs

    monkeypatch.setattr(airflow_fs, "_register_filesystems", lambda: {
        "file": lambda conn_id, options: __import__("fsspec").implementations.local.LocalFileSystem(**options),
        "memory": lambda conn_id, options: MemoryFileSystem(**options),
    })
    uri = "memory://hf-provider-tests/round-trip.json"
    reference = _save(Dataset.from_dict({"value": [3]}), uri)
    restored = _load_artifact(reference)
    assert restored["value"] == [3]
