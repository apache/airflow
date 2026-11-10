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
"""Operators to load, transform, and publish Hugging Face datasets."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import marshal
import re
import tempfile
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datasets import Dataset, DatasetDict

from airflow.providers.common.compat.sdk import Asset, BaseOperator, Context, ObjectStoragePath
from airflow.providers.huggingface.hooks.huggingface import HuggingFaceHook


def _path(uri: str, *, conn_id: str | None = None) -> ObjectStoragePath:
    """Resolve a URI through Airflow's configured object-storage filesystems."""
    return ObjectStoragePath(uri, conn_id=conn_id)


def _reference(reference: str | dict[str, Any]) -> str:
    if isinstance(reference, dict):
        uri = reference.get("uri")
        if not uri:
            raise ValueError("Artifact reference must contain a non-empty 'uri'.")
        return uri
    if not isinstance(reference, str) or not reference:
        raise ValueError("Artifact reference must be a URI or contain a 'uri'.")
    return reference


class _HashingWriter:
    def __init__(self, stream: Any) -> None:
        self._stream = stream
        self._digest = hashlib.sha256()

    def write(self, data: bytes) -> int:
        written = self._stream.write(data)
        self._digest.update(data[:written])
        return written

    def get_hexdigest(self) -> str:
        return self._digest.hexdigest()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)


def _validate_feature(feature: Any, field_name: str) -> None:
    """Allow Parquet-safe scalar, class-label, and nested sequence features."""
    from datasets import ClassLabel, LargeList, Sequence, Value

    if isinstance(feature, (Value, ClassLabel)):
        return
    if isinstance(feature, (Sequence, LargeList)):
        _validate_feature(feature.feature, field_name)
        return
    if isinstance(feature, dict):
        for key, value in feature.items():
            _validate_feature(value, f"{field_name}.{key}")
        return
    if isinstance(feature, (list, tuple)):
        for value in feature:
            _validate_feature(value, field_name)
        return
    raise ValueError(
        f"unsupported Parquet feature type for {field_name!r}: {type(feature).__name__}; "
        "supported types are Value, ClassLabel, and nested Sequence or LargeList features"
    )


def _dataset_parts(dataset: Dataset | DatasetDict) -> list[tuple[str, Dataset]]:
    from datasets import Dataset, DatasetDict

    if isinstance(dataset, DatasetDict):
        return list(dataset.items())
    if isinstance(dataset, Dataset):
        return [("dataset", dataset)]
    raise TypeError("Expected a datasets.Dataset or datasets.DatasetDict.")


def _save(
    dataset: Dataset | DatasetDict,
    uri: str,
    metadata: dict[str, Any] | None = None,
    *,
    storage_conn_id: str | None = None,
) -> dict[str, Any]:
    """Write split Parquet objects and a JSON manifest through ObjectStoragePath."""
    import pyarrow.parquet as parquet

    parts = _dataset_parts(dataset)
    written: list[ObjectStoragePath] = []
    manifest_path = _path(uri, conn_id=storage_conn_id)
    split_manifest: dict[str, Any] = {}
    try:
        for index, (name, part) in enumerate(parts):
            for field_name, feature in part.features.items():
                _validate_feature(feature, field_name)
            split_uri = f"{uri}.split-{index:05d}.parquet"
            split_path = _path(split_uri, conn_id=storage_conn_id)
            split_path.parent.mkdir(parents=True, exist_ok=True)
            with split_path.open("wb") as stream:
                hashing_stream = _HashingWriter(stream)
                parquet.write_table(part.data.table, hashing_stream)
            written.append(split_path)
            split_manifest[name] = {
                "uri": split_uri,
                "rows": len(part),
                "features": part.features.to_dict(),
                "feature_order": list(part.features),
                "split": (
                    str(part.split)
                    if getattr(part, "split", None) is not None
                    else (name if name != "dataset" else None)
                ),
                "sha256": hashing_stream.get_hexdigest(),
            }

        manifest = {
            "format": "parquet",
            "version": 1,
            "splits": split_manifest,
            "metadata": metadata or {},
        }
        manifest_bytes = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with manifest_path.open("wb") as stream:
            stream.write(manifest_bytes)
        return {
            "uri": uri,
            "format": "parquet",
            "splits": {
                name: {
                    "rows": part_info["rows"],
                    "features": part_info["features"],
                    "split": part_info["split"],
                }
                for name, part_info in split_manifest.items()
            },
            **(metadata or {}),
            "fingerprint": hashlib.sha256(manifest_bytes).hexdigest(),
        }
    except Exception:
        # Remove partial split outputs. A failed write must not leave an artifact
        # reference that a downstream task could mistake for a complete dataset.
        for path in written:
            with suppress(OSError, NotImplementedError):
                path.unlink(missing_ok=True)
        with suppress(OSError, NotImplementedError):
            manifest_path.unlink(missing_ok=True)
        raise


def _read_artifact_manifest(
    reference: str | dict[str, Any], *, storage_conn_id: str | None = None
) -> tuple[str, dict[str, Any]]:
    uri = _reference(reference)
    manifest_path = _path(uri, conn_id=storage_conn_id)
    with manifest_path.open("rb") as stream:
        manifest_bytes = stream.read()
    if isinstance(reference, dict) and reference.get("fingerprint"):
        actual = hashlib.sha256(manifest_bytes).hexdigest()
        if actual != reference["fingerprint"]:
            raise ValueError("Dataset artifact fingerprint does not match its reference")

    manifest = json.loads(manifest_bytes)
    if manifest.get("format") != "parquet" or manifest.get("version") != 1:
        raise ValueError(f"Unsupported dataset artifact manifest: {uri}")
    return uri, manifest


def _load_artifact(
    reference: str | dict[str, Any],
    *,
    cache_dir: str | None = None,
    manifest: dict[str, Any] | None = None,
    storage_conn_id: str | None = None,
) -> Dataset | DatasetDict:
    from datasets import Dataset, DatasetDict, Features

    uri, manifest = (
        _read_artifact_manifest(reference, storage_conn_id=storage_conn_id)
        if manifest is None
        else (_reference(reference), manifest)
    )
    resolved_cache_dir = cache_dir or str(Path(tempfile.gettempdir()) / "airflow-hf-datasets-cache")
    Path(resolved_cache_dir).mkdir(parents=True, exist_ok=True)
    parts: dict[str, Dataset] = {}
    with tempfile.TemporaryDirectory(prefix="airflow-hf-parquet-") as local_dir:
        for index, (name, item) in enumerate(manifest["splits"].items()):
            split_uri = item["uri"]
            local_path = Path(local_dir) / f"split-{index:05d}.parquet"
            digest = hashlib.sha256()
            with (
                _path(split_uri, conn_id=storage_conn_id).open("rb") as source,
                local_path.open("wb") as target,
            ):
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
                    target.write(chunk)
            if digest.hexdigest() != item.get("sha256"):
                raise ValueError(f"Dataset artifact split fingerprint mismatch: {split_uri}")

            feature_data = item["features"]
            feature_order = item.get("feature_order", list(feature_data))
            features = Features.from_dict({key: feature_data[key] for key in feature_order})
            split_name = item.get("split")
            split_kwargs = (
                {"split": split_name}
                if isinstance(split_name, str) and re.fullmatch(r"\w+(?:\.\w+)*", split_name)
                else {}
            )
            if item.get("rows") == 0:
                restored = Dataset.from_dict({key: [] for key in features}, features=features, **split_kwargs)
            else:
                restored = Dataset.from_parquet(
                    str(local_path),
                    features=features,
                    cache_dir=resolved_cache_dir,
                    **split_kwargs,
                )
            # Parquet readers may return fields in physical/file order; restore the
            # manifest's declared feature order for stable downstream transforms.
            parts[name] = restored.select_columns(list(features))
    return parts["dataset"] if list(parts) == ["dataset"] else DatasetDict(parts)


def _state_key(prefix: str, identity: str) -> str:
    """Build a path-safe task-state key that fits the task state store column."""
    digest = hashlib.sha256(identity.encode()).hexdigest()
    return f"{prefix}:{digest}"


def _publication_marker(prefix: str, identity: dict[str, Any]) -> str:
    payload = json.dumps(identity, sort_keys=True, separators=(",", ":"), default=str).encode()
    return f"{prefix}:{hashlib.sha256(payload).hexdigest()}"


def _callable_identity(function: Callable[..., Any] | None) -> dict[str, str] | None:
    if function is None:
        return None
    code = getattr(function, "__code__", None)
    if code is None:
        code = getattr(type(function).__call__, "__code__", None)
    return {
        "module": getattr(function, "__module__", type(function).__module__),
        "qualname": getattr(function, "__qualname__", type(function).__qualname__),
        "code_hash": hashlib.sha256(marshal.dumps(code)).hexdigest() if code is not None else "",
    }


def _warn_missing_task_state(operator: BaseOperator, state: Any) -> None:
    if getattr(operator, "durable", False) and state is None:
        operator.log.warning(
            "durable=True requires Airflow 3.3 task state store support; revision pinning and "
            "task-state result caching are unavailable for this task run"
        )


def _commit_details(commit: Any, repo_id: str, hook: HuggingFaceHook) -> tuple[str, str]:
    """Normalize CommitInfo and GitCommitInfo returned by Hub APIs."""
    commit_sha = getattr(commit, "oid", None) or getattr(commit, "commit_id", None)
    if not commit_sha:
        raise ValueError(f"Hub publication for {repo_id!r} returned no commit identifier")
    commit_url = getattr(commit, "commit_url", None)
    if not commit_url:
        endpoint = getattr(hook.get_conn(), "endpoint", "https://huggingface.co").rstrip("/")
        commit_url = f"{endpoint}/datasets/{repo_id}/commit/{commit_sha}"
    return commit_sha, commit_url


def _is_hub_repo(path: str) -> bool:
    """Treat non-local paths as Hub IDs unless they name a packaged builder."""
    if Path(path).exists():
        return False
    if "/" in path:
        return True
    try:
        return importlib.util.find_spec(f"datasets.packaged_modules.{path}") is None
    except (ImportError, ModuleNotFoundError, ValueError):
        return True


def _publication_outlets(
    repo_id: str, outlets: Any, push_kwargs: dict[str, Any]
) -> tuple[Asset | None, list[Any]]:
    declared_outlets = list(outlets) if isinstance(outlets, (list, tuple)) else ([outlets] if outlets else [])
    asset_uri = f"hf://datasets/{repo_id}"
    if push_kwargs.get("create_pr") or push_kwargs.get("revision", "main") not in (None, "main"):
        # A mapped task declares the repo asset in partial() before its push_kwargs are
        # expanded; drop it here so the unmapped task does not emit a repo-level event.
        return None, [outlet for outlet in declared_outlets if getattr(outlet, "uri", None) != asset_uri]

    existing_asset = next(
        (outlet for outlet in declared_outlets if getattr(outlet, "uri", None) == asset_uri), None
    )
    asset = existing_asset or Asset(uri=asset_uri)
    if existing_asset is None:
        declared_outlets.append(asset)
    return asset, declared_outlets


class LoadDatasetOperator(BaseOperator):
    """Load a dataset revision and persist it as a Parquet artifact."""

    template_fields = (
        "path",
        "name",
        "split",
        "revision",
        "cache_dir",
        "output_uri",
        "storage_conn_id",
        "huggingface_conn_id",
    )

    def __init__(
        self,
        *,
        path: str,
        output_uri: str,
        name: str | None = None,
        split: str | None = None,
        revision: str | None = None,
        cache_dir: str | None = None,
        storage_conn_id: str | None = None,
        download_mode: str = "reuse_dataset_if_exists",
        huggingface_conn_id: str | None = "huggingface_default",
        load_kwargs: dict[str, Any] | None = None,
        durable: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.output_uri = output_uri
        self.name = name
        self.split = split
        self.revision = revision
        self.cache_dir = cache_dir
        self.download_mode = download_mode
        self.storage_conn_id = storage_conn_id
        self.huggingface_conn_id = huggingface_conn_id
        self.load_kwargs = load_kwargs or {}
        self.durable = durable

    def execute(self, context: Context) -> dict[str, Any]:
        hook = HuggingFaceHook(self.huggingface_conn_id)
        requested_revision = self.revision
        is_hub_repo = _is_hub_repo(self.path)
        state = context.get("task_state_store") if self.durable else None
        _warn_missing_task_state(self, state)
        state_identity = json.dumps([self.path, requested_revision], separators=(",", ":"))
        state_key = _state_key("load-revision", state_identity)
        revision = state.get(state_key) if state is not None else None
        if revision is None:
            revision = requested_revision
            if is_hub_repo:
                revision = hook.get_dataset_info(self.path, requested_revision).sha
            if state is not None and revision is not None:
                state.set(state_key, revision)
        elif state is not None:
            self.log.info("Reusing pinned Hugging Face dataset revision %s for %s", revision, self.path)

        kwargs = dict(self.load_kwargs)
        kwargs["download_mode"] = self.download_mode
        if self.name is not None:
            kwargs["name"] = self.name
        if self.split is not None:
            kwargs["split"] = self.split
        if revision is not None:
            kwargs["revision"] = revision
        if self.cache_dir is not None:
            kwargs["cache_dir"] = self.cache_dir
        dataset = hook.load_dataset(self.path, **kwargs)
        metadata = {
            "repository_id": self.path if is_hub_repo else None,
            "revision": revision,
            "configuration": self.name,
            "split": self.split,
        }
        return _save(dataset, self.output_uri, metadata, storage_conn_id=self.storage_conn_id)


class StreamDatasetOperator(BaseOperator):
    """Transform a streamed dataset lazily and publish it directly to the Hub."""

    template_fields = (
        "path",
        "name",
        "split",
        "revision",
        "config",
        "commit_message",
        "huggingface_conn_id",
    )

    @classmethod
    def partial(cls, **kwargs: Any) -> Any:
        repo_id = kwargs.get("repo_id")
        if not isinstance(repo_id, str):
            raise ValueError(
                "repo_id must be fixed in partial() so its publication asset is known at Dag parse time"
            )
        if "{{" in repo_id:
            raise ValueError(
                "repo_id cannot be templated: it defines the Asset that downstream DAGs schedule on."
            )
        _, kwargs["outlets"] = _publication_outlets(
            repo_id, kwargs.get("outlets"), kwargs.get("push_kwargs") or {}
        )
        return super().partial(**kwargs)

    def __init__(
        self,
        *,
        path: str,
        repo_id: str,
        transform: Callable[..., dict[str, Any]] | None = None,
        predicate: Callable[[dict[str, Any]], bool] | None = None,
        name: str | None = None,
        split: str | None = None,
        revision: str | None = None,
        config: str | None = None,
        private: bool | None = None,
        commit_message: str | None = None,
        transform_kwargs: dict[str, Any] | None = None,
        push_kwargs: dict[str, Any] | None = None,
        huggingface_conn_id: str | None = "huggingface_default",
        durable: bool = True,
        **kwargs: Any,
    ) -> None:
        if "{{" in repo_id:
            raise ValueError(
                "repo_id cannot be templated: it defines the Asset that downstream DAGs schedule on."
            )
        if transform is not None and not callable(transform):
            raise TypeError("transform must be a callable or None")
        if predicate is not None and not callable(predicate):
            raise TypeError("predicate must be a callable or None")
        self.path = path
        self.repo_id = repo_id
        self.transform, self.predicate = transform, predicate
        self.name = name
        self.split = split
        self.revision = revision
        self.config = config
        self.private = private
        self.commit_message = commit_message
        self.transform_kwargs = transform_kwargs or {}
        self.push_kwargs = push_kwargs or {}
        self.huggingface_conn_id = huggingface_conn_id
        self.durable = durable
        self.published_asset, declared_outlets = _publication_outlets(
            repo_id, kwargs.pop("outlets", None), self.push_kwargs
        )
        super().__init__(outlets=declared_outlets, **kwargs)

    def execute(self, context: Context) -> dict[str, Any]:
        hook = HuggingFaceHook(self.huggingface_conn_id)
        is_hub_repo = _is_hub_repo(self.path)
        requested_revision = self.revision
        state = context.get("task_state_store") if self.durable else None
        _warn_missing_task_state(self, state)
        revision_identity = json.dumps(
            [self.path, requested_revision, self.name, self.split], separators=(",", ":")
        )
        revision_key = _state_key("stream-revision", revision_identity)
        revision = state.get(revision_key) if state is not None else None
        if revision is None:
            revision = requested_revision
            if is_hub_repo:
                revision = hook.get_dataset_info(self.path, requested_revision).sha
            if state is not None and revision is not None:
                state.set(revision_key, revision)
        elif state is not None:
            self.log.info("Reusing pinned Hugging Face streaming revision %s for %s", revision, self.path)

        run_id = context.get("run_id", "manual")
        dag = context.get("dag")
        task_instance = context.get("ti")
        push_kwargs = dict(self.push_kwargs)
        if self.config is not None:
            push_kwargs["config_name"] = self.config
        if self.private is not None:
            push_kwargs["private"] = self.private
        create_pr = bool(push_kwargs.get("create_pr", False))
        marker = _publication_marker(
            "huggingface-stream-publish",
            {
                "task_id": self.task_id,
                "dag_id": getattr(dag, "dag_id", None),
                "map_index": getattr(task_instance, "map_index", -1),
                "run_id": run_id,
                "source": self.path,
                "source_revision": revision,
                "name": self.name,
                "split": self.split,
                "transform": _callable_identity(self.transform),
                "transform_kwargs": self.transform_kwargs,
                "predicate": _callable_identity(self.predicate),
                "repo_id": self.repo_id,
                "config_name": push_kwargs.get("config_name", "default"),
                "revision": push_kwargs.get("revision", "main"),
                "create_pr": create_pr,
            },
        )
        state_key = _state_key("stream-publication", marker)
        pending_key = _state_key("stream-publication-pending", marker)
        if state is not None:
            prior_result = state.get(state_key)
            if isinstance(prior_result, dict):
                self.log.info("Reusing stored streaming publication result for %s", self.repo_id)
                self._emit_asset_event(context, prior_result)
                return prior_result

        commit = None
        recovery_pending = state is not None and bool(state.get(pending_key))
        if recovery_pending:
            commit = hook.find_commit_by_marker(
                self.repo_id,
                marker,
                revision=push_kwargs.get("revision"),
                include_pull_requests=create_pr,
            )
            if commit is not None:
                self.log.info("Reusing existing completed streaming publication to %s", self.repo_id)

        if commit is None:
            load_kwargs: dict[str, Any] = {"streaming": True}
            if self.name is not None:
                load_kwargs["name"] = self.name
            if self.split is not None:
                load_kwargs["split"] = self.split
            if revision is not None:
                load_kwargs["revision"] = revision
            stream = hook.load_dataset(self.path, **load_kwargs)

            if self.transform is not None:
                stream = stream.map(self.transform, fn_kwargs=self.transform_kwargs)
            if self.predicate is not None:
                stream = stream.filter(self.predicate)

            message = self.commit_message or "Stream dataset from Airflow"
            push_kwargs["commit_message"] = f"{message} [{marker}]"
            if state is not None:
                state.set(pending_key, True)
            commit = hook.push_to_hub(stream, self.repo_id, **push_kwargs)

        commit_sha, commit_url = _commit_details(commit, self.repo_id, hook)
        result = {
            "repository_id": self.repo_id,
            "commit_sha": commit_sha,
            "commit_url": commit_url,
            "url": f"https://huggingface.co/datasets/{self.repo_id}",
            "source_repository_id": self.path if is_hub_repo else None,
            "source_revision": revision,
            "configuration": self.name,
            "split": self.split,
            "streaming": True,
        }
        if state is not None:
            state.set(state_key, result)
        self._emit_asset_event(context, result)
        return result

    def _emit_asset_event(self, context: Context, publication: dict[str, Any]) -> None:
        events = context.get("outlet_events")
        if events is not None and self.published_asset is not None:
            events[self.published_asset].extra = {
                "repository_id": self.repo_id,
                "commit_sha": publication["commit_sha"],
                "commit_url": publication["commit_url"],
                "source_revision": publication["source_revision"],
            }


class MapDatasetOperator(BaseOperator):
    """Apply a Python callable and persist the transformed artifact."""

    template_fields = ("input_uri", "output_uri", "transform_kwargs", "cache_dir", "storage_conn_id")

    def __init__(
        self,
        *,
        input_uri: str | dict[str, Any],
        output_uri: str,
        transform: Callable[..., Any],
        transform_kwargs: dict[str, Any] | None = None,
        map_kwargs: dict[str, Any] | None = None,
        cache_dir: str | None = None,
        storage_conn_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if not callable(transform):
            raise TypeError("transform must be a callable")
        self.input_uri = input_uri
        self.output_uri = output_uri
        self.transform = transform
        self.transform_kwargs = transform_kwargs
        self.map_kwargs = map_kwargs or {}
        self.cache_dir = cache_dir
        self.storage_conn_id = storage_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="airflow-hf-cache-") as cache_dir:
            dataset = _load_artifact(
                self.input_uri,
                cache_dir=self.cache_dir or cache_dir,
                storage_conn_id=self.storage_conn_id,
            )
            transform_kwargs = self.transform_kwargs if self.transform_kwargs is not None else {}
            mapped = dataset.map(self.transform, fn_kwargs=transform_kwargs, **self.map_kwargs)
            source = self.input_uri if isinstance(self.input_uri, dict) else {}
            provenance: dict[str, Any] = {
                key: source[key]
                for key in ("repository_id", "revision", "configuration", "split")
                if key in source
            }
            return _save(mapped, self.output_uri, provenance, storage_conn_id=self.storage_conn_id)


class PushToHubOperator(BaseOperator):
    """Synchronously publish an artifact and emit its successful asset event."""

    template_fields = ("input_uri", "config", "commit_message", "huggingface_conn_id", "storage_conn_id")

    @classmethod
    def partial(cls, **kwargs: Any) -> Any:
        repo_id = kwargs.get("repo_id")
        if not isinstance(repo_id, str):
            raise ValueError(
                "repo_id must be fixed in partial() so its publication asset is known at Dag parse time"
            )
        if "{{" in repo_id:
            raise ValueError(
                "repo_id cannot be templated: it defines the Asset that downstream DAGs schedule on."
            )
        _, kwargs["outlets"] = _publication_outlets(
            repo_id, kwargs.get("outlets"), kwargs.get("push_kwargs") or {}
        )
        return super().partial(**kwargs)

    def __init__(
        self,
        *,
        input_uri: dict[str, Any],
        repo_id: str,
        config: str | None = None,
        private: bool | None = None,
        commit_message: str | None = None,
        huggingface_conn_id: str | None = "huggingface_default",
        storage_conn_id: str | None = None,
        push_kwargs: dict[str, Any] | None = None,
        durable: bool = True,
        **kwargs: Any,
    ) -> None:
        if "{{" in repo_id:
            raise ValueError(
                "repo_id cannot be templated: it defines the Asset that downstream DAGs schedule on."
            )
        self.input_uri = input_uri
        self.repo_id = repo_id
        self.config = config
        self.private = private
        self.commit_message = commit_message
        self.huggingface_conn_id = huggingface_conn_id
        self.push_kwargs = push_kwargs or {}
        self.storage_conn_id = storage_conn_id
        self.durable = durable
        self.published_asset, declared_outlets = _publication_outlets(
            repo_id, kwargs.pop("outlets", None), self.push_kwargs
        )
        super().__init__(outlets=declared_outlets, **kwargs)

    def execute(self, context: Context) -> dict[str, Any]:
        source = self.input_uri if isinstance(self.input_uri, dict) else {}
        fingerprint = source.get("fingerprint")
        if not fingerprint:
            raise ValueError("Publication requires an artifact reference with a fingerprint")
        kwargs = dict(self.push_kwargs)
        if self.config is not None:
            kwargs["config_name"] = self.config
        if self.private is not None:
            kwargs["private"] = self.private
        target_revision = kwargs.get("revision")
        create_pr = bool(kwargs.get("create_pr", False))
        target_marker = _publication_marker(
            "target",
            {
                "repo_id": self.repo_id,
                "config_name": kwargs.get("config_name", "default"),
                "revision": target_revision or "main",
                "create_pr": create_pr,
                "push_kwargs": {key: value for key, value in kwargs.items() if key != "commit_message"},
            },
        )
        marker = f"airflow-artifact-sha256:{fingerprint}:{target_marker}"
        state = context.get("task_state_store") if self.durable else None
        _warn_missing_task_state(self, state)
        state_key = _state_key("publication", marker)
        pending_key = _state_key("publication-pending", marker)
        if state is not None:
            prior_result = state.get(state_key)
            if isinstance(prior_result, dict):
                self.log.info("Reusing stored publication result for %s", self.repo_id)
                self._emit_asset_event(context, prior_result)
                return prior_result

        hook = HuggingFaceHook(self.huggingface_conn_id)
        commit = None
        recovery_pending = state is not None and bool(state.get(pending_key))
        if recovery_pending:
            commit = hook.find_commit_by_marker(
                self.repo_id,
                marker,
                revision=target_revision,
                include_pull_requests=create_pr,
            )
            if commit is not None:
                self.log.info("Reusing existing completed publication to %s", self.repo_id)
        if commit is None:
            with tempfile.TemporaryDirectory(prefix="airflow-hf-cache-") as cache_dir:
                dataset = _load_artifact(
                    self.input_uri,
                    cache_dir=cache_dir,
                    storage_conn_id=self.storage_conn_id,
                )
                message = self.commit_message or "Publish dataset from Airflow"
                kwargs["commit_message"] = f"{message} [{marker}]"
                if state is not None:
                    state.set(pending_key, True)
                commit = hook.push_to_hub(dataset, self.repo_id, **kwargs)

        commit_sha, commit_url = _commit_details(commit, self.repo_id, hook)
        result = {
            "repository_id": self.repo_id,
            "commit_sha": commit_sha,
            "commit_url": commit_url,
            "url": f"https://huggingface.co/datasets/{self.repo_id}",
            "fingerprint": fingerprint,
            "source_repository_id": source.get("repository_id"),
            "source_revision": source.get("revision"),
        }
        if state is not None:
            state.set(state_key, result)
        self._emit_asset_event(context, result)
        return result

    def _emit_asset_event(self, context: Context, publication: dict[str, Any]) -> None:
        events = context.get("outlet_events")
        if events is not None and self.published_asset is not None:
            events[self.published_asset].extra = {
                "repository_id": self.repo_id,
                "commit_sha": publication["commit_sha"],
                "commit_url": publication["commit_url"],
                "fingerprint": publication["fingerprint"],
            }
