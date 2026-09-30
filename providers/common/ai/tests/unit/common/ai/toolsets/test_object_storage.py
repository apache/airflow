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
from __future__ import annotations

import asyncio
import gzip
import json
import uuid
from typing import Any
from unittest.mock import patch

import pytest
from fsspec.implementations.memory import MemoryFileSystem
from pydantic_ai import RunContext
from pydantic_ai.exceptions import ToolFailed
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.toolsets.object_storage import ObjectStorageToolset
from airflow.sdk.io.store import _STORE_CACHE, ObjectStore


@pytest.fixture
def storage(tmp_path):
    """A small tree of files to read, with a sibling directory the agent must not reach."""
    root = tmp_path / "reports"
    (root / "2026" / "09").mkdir(parents=True)
    (root / "2026" / "09" / "summary.md").write_text("# September\n\nRevenue up 4%.\n")
    (root / "orders.csv").write_text("id,total\n1,10\n2,20\n")
    (root / "config.yaml").write_text("retries: 3\n")
    (root / "notes.txt.gz").write_bytes(gzip.compress(b"compressed notes\n"))
    (root / "logo.png").write_bytes(b"\x89PNG\r\n\x1a\n")
    (root / "blob.bin").write_bytes(b"\x00\x01\x02")
    secret = tmp_path / "secrets"
    secret.mkdir()
    (secret / "key.txt").write_text("do not read\n")
    (root / "link").symlink_to(secret / "key.txt")
    return root


@pytest.fixture
def object_store(monkeypatch):
    """
    A root on an in-memory object store, with a file above it the agent must not reach.

    No symlinks and no local disk, so only the toolset's own path check keeps a path inside.
    """
    conn_id = f"memory-{uuid.uuid4().hex}"
    fs = MemoryFileSystem()
    monkeypatch.setitem(_STORE_CACHE, f"memory-{conn_id}", ObjectStore("memory", conn_id, fs=fs))
    bucket = f"/{conn_id}"
    fs.pipe(f"{bucket}/reports/a/ok.txt", b"inside\n")
    fs.pipe(f"{bucket}/secret.txt", b"do not read\n")
    fs.pipe(f"{bucket}/reports/..%2Fsecret.txt", b"a key inside the root\n")
    yield ObjectStorageToolset(f"memory:/{bucket}/reports", conn_id=conn_id)
    fs.rm(bucket, recursive=True)


def _call(toolset: ObjectStorageToolset, name: str, arguments: dict[str, Any]) -> Any:
    async def call() -> Any:
        ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
        tools = await toolset.get_tools(ctx)
        validated = tools[name].args_validator.validate_python(arguments)
        return await toolset.call_tool(name, validated, ctx, tools[name])

    return asyncio.run(call())


class TestTools:
    def test_exposes_three_read_only_tools(self, storage):
        ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())

        tools = asyncio.run(ObjectStorageToolset(f"file://{storage}").get_tools(ctx))

        assert list(tools) == ["list_files", "get_file_info", "read_file"]

    def test_the_id_names_the_connection(self, storage):
        assert (
            ObjectStorageToolset(f"file://{storage}", conn_id="reports_s3").id == "object-storage-reports_s3"
        )

    def test_a_prefix_renames_the_tools(self, storage):
        ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())

        tools = asyncio.run(ObjectStorageToolset(f"file://{storage}", tool_prefix="reports").get_tools(ctx))

        assert list(tools) == ["reports_list_files", "reports_get_file_info", "reports_read_file"]
        listing = json.loads(
            _call(ObjectStorageToolset(f"file://{storage}", tool_prefix="reports"), "reports_list_files", {})
        )
        assert listing["path"] == "/"

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"max_files": 0}, "max_files"),
            ({"max_read_bytes": 0}, "max_read_bytes"),
            ({"tool_prefix": "my-files"}, "tool_prefix"),
        ],
    )
    def test_rejects_an_invalid_setting(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            ObjectStorageToolset("file:///tmp", **kwargs)


class TestListFiles:
    def test_lists_one_directory_with_sizes(self, storage):
        listing = json.loads(_call(ObjectStorageToolset(f"file://{storage}"), "list_files", {}))

        names = [entry["name"] for entry in listing["entries"]]
        assert names == sorted(names)
        assert "2026/" in names
        assert {"name": "orders.csv", "size_bytes": 19} in listing["entries"]

    def test_lists_a_subdirectory(self, storage):
        listing = json.loads(
            _call(ObjectStorageToolset(f"file://{storage}"), "list_files", {"path": "2026/09"})
        )

        assert listing["entries"] == [{"name": "summary.md", "size_bytes": 28}]

    def test_pages_through_a_directory_larger_than_max_files(self, storage):
        toolset = ObjectStorageToolset(f"file://{storage}", max_files=2)

        first = json.loads(_call(toolset, "list_files", {}))
        second = json.loads(_call(toolset, "list_files", {"offset": 2}))

        assert len(first["entries"]) == 2
        assert first["note"].endswith("list again with offset=2 for more.")
        assert {e["name"] for e in first["entries"]}.isdisjoint(e["name"] for e in second["entries"])

    def test_leaves_out_a_symlink_that_leads_out_of_the_root(self, storage):
        listing = json.loads(_call(ObjectStorageToolset(f"file://{storage}"), "list_files", {}))

        assert "link" not in {entry["name"] for entry in listing["entries"]}

    def test_lists_a_child_with_the_same_name_as_its_directory(self, storage):
        (storage / "data").mkdir()
        (storage / "data" / "data").write_text("x")

        listing = json.loads(_call(ObjectStorageToolset(f"file://{storage}"), "list_files", {"path": "data"}))

        assert listing["entries"] == [{"name": "data", "size_bytes": 1}]

    def test_refuses_a_file_as_a_directory(self, storage):
        with pytest.raises(ToolFailed, match="is not a directory"):
            _call(ObjectStorageToolset(f"file://{storage}"), "list_files", {"path": "orders.csv"})


class TestPathsStayUnderTheRoot:
    @pytest.mark.parametrize(
        ("path", "match"),
        [
            ("../secrets/key.txt", "leaves the storage root"),
            ("2026/../../secrets/key.txt", "leaves the storage root"),
            ("/etc/passwd", "is not a relative path"),
            ("file:///etc/passwd", "is not a relative path"),
            ("link", "resolves outside the storage root"),
        ],
    )
    def test_refuses_a_path_outside_the_root(self, storage, path, match):
        with pytest.raises(ToolFailed, match=match):
            _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": path})

    @pytest.mark.parametrize("tool", ["read_file", "get_file_info"])
    def test_a_root_written_without_a_scheme_is_checked_for_symlinks_too(self, storage, tool):
        """A plain path is still on the worker's disk, where the symlink leads anywhere."""
        with pytest.raises(ToolFailed, match="resolves outside the storage root"):
            _call(ObjectStorageToolset(str(storage)), tool, {"path": "link"})

    def test_a_sibling_whose_name_starts_with_the_root_is_outside_it(self, storage):
        (storage.parent / "reports-private").mkdir()
        (storage.parent / "reports-private" / "k.txt").write_text("x")
        (storage / "near").symlink_to(storage.parent / "reports-private" / "k.txt")

        with pytest.raises(ToolFailed, match="resolves outside the storage root"):
            _call(ObjectStorageToolset(str(storage)), "read_file", {"path": "near"})


class TestPathsStayUnderAnObjectStoreRoot:
    """An object store has no directories to climb out of: a key is only ever a string."""

    def test_dot_dot_is_refused(self, object_store):
        with pytest.raises(ToolFailed, match="leaves the storage root"):
            _call(object_store, "read_file", {"path": "../secret.txt"})

    @pytest.mark.parametrize(
        "path",
        ["%2e%2e/secret.txt", "a/%2e%2e/%2e%2e/secret.txt", "..\\secret.txt", "a\\..\\..\\secret.txt"],
    )
    def test_encoded_dots_and_backslashes_do_not_climb_out(self, object_store, path):
        """Neither is decoded or treated as a separator, so the key sits under the root and is missing."""
        with pytest.raises(ToolFailed, match="is not a file"):
            _call(object_store, "read_file", {"path": path})

    def test_a_key_that_looks_encoded_is_read_under_the_root(self, object_store):
        assert _call(object_store, "read_file", {"path": "..%2Fsecret.txt"}) == "a key inside the root"


class TestReadFile:
    def test_reads_a_text_file(self, storage):
        result = _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": "2026/09/summary.md"})

        assert result == "# September\n\nRevenue up 4%."

    def test_reads_a_file_with_an_extension_file_analysis_does_not_know(self, storage):
        assert (
            _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": "config.yaml"})
            == "retries: 3"
        )

    def test_decompresses_a_compressed_text_file(self, storage):
        result = _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": "notes.txt.gz"})

        assert result == "compressed notes"

    def test_decompresses_text_with_an_extension_file_analysis_does_not_know(self, storage):
        (storage / "schema.sql.gz").write_bytes(gzip.compress(b"CREATE TABLE t (id INT);\n"))

        result = _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": "schema.sql.gz"})

        assert result == "CREATE TABLE t (id INT);"

    def test_a_corrupt_file_is_refused_instead_of_failing_the_run(self, storage):
        (storage / "broken.txt.gz").write_bytes(b"not gzip at all")

        with pytest.raises(ToolFailed, match="cannot be read"):
            _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": "broken.txt.gz"})

    @pytest.mark.parametrize(
        ("path", "module", "error"),
        [
            pytest.param("broken.parquet", "pyarrow.parquet", "ArrowInvalid", id="parquet"),
            pytest.param("broken.avro", "fastavro", "ValueError", id="avro"),
        ],
    )
    def test_a_corrupt_columnar_file_is_refused_instead_of_failing_the_run(
        self, storage, path, module, error
    ):
        """pyarrow's ArrowInvalid is a ValueError, so the refusal covers Parquet as well as gzip."""
        pytest.importorskip(module)
        (storage / path).write_bytes(b"neither parquet nor avro")

        with pytest.raises(ToolFailed, match=f"cannot be read: {error}"):
            _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": path})

    def test_reads_the_schema_and_first_rows_of_a_parquet_file(self, storage):
        pq = pytest.importorskip("pyarrow.parquet")
        pa = pytest.importorskip("pyarrow")
        pq.write_table(
            pa.table({"id": list(range(100)), "total": [i * 10 for i in range(100)]}),
            storage / "orders.parquet",
        )

        result = _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": "orders.parquet"})

        assert result.startswith("Schema: id: int64, total: int64")
        assert '"id": 19' in result
        assert '"id": 20' not in result

    def test_reads_a_window_and_says_where_to_continue(self, storage):
        result = _call(
            ObjectStorageToolset(f"file://{storage}"),
            "read_file",
            {"path": "orders.csv", "offset": 2, "limit": 1},
        )

        assert result == "1,10\n[... 1 more line; read on with offset=3]"

    @pytest.mark.parametrize(
        ("path", "match"),
        [
            ("logo.png", "png file"),
            ("blob.bin", "binary file"),
            ("missing.txt", "not a file"),
            ("2026", "not a file"),
        ],
    )
    def test_refuses_what_it_cannot_read_as_text(self, storage, path, match):
        with pytest.raises(ToolFailed, match=match):
            _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": path})

    def test_refuses_a_file_over_the_read_limit(self, storage):
        with pytest.raises(ToolFailed, match="larger than the 8B"):
            _call(
                ObjectStorageToolset(f"file://{storage}", max_read_bytes=8),
                "read_file",
                {"path": "orders.csv"},
            )

    @pytest.mark.enable_redact
    def test_a_secret_spanning_lines_is_masked_before_a_window_is_cut(self, storage, register_secret):
        secret = register_secret("pem-part-one-91c3\npem-part-two-91c3")
        (storage / "key.pem").write_text(f"header\n{secret}\nfooter\n")

        first_line_of_secret = _call(
            ObjectStorageToolset(f"file://{storage}"),
            "read_file",
            {"path": "key.pem", "offset": 2, "limit": 1},
        )

        assert "pem-part-one-91c3" not in first_line_of_secret


class TestGetFileInfo:
    def test_describes_a_file(self, storage):
        info = json.loads(
            _call(ObjectStorageToolset(f"file://{storage}"), "get_file_info", {"path": "orders.csv"})
        )

        assert info["type"] == "file"
        assert info["size_bytes"] == 19

    def test_a_missing_path_is_refused(self, storage):
        with pytest.raises(ToolFailed, match="does not exist"):
            _call(ObjectStorageToolset(f"file://{storage}"), "get_file_info", {"path": "missing.txt"})

    def test_the_modification_time_is_iso_8601(self, storage):
        info = json.loads(
            _call(ObjectStorageToolset(f"file://{storage}"), "get_file_info", {"path": "orders.csv"})
        )

        assert info["modified"].endswith("+00:00")

    def test_describes_a_directory(self, storage):
        info = json.loads(_call(ObjectStorageToolset(f"file://{storage}"), "get_file_info", {"path": "2026"}))

        assert info == {"path": "2026", "type": "directory"}


class TestOtherFrameworks:
    def test_a_refusal_reaches_a_native_agent_as_an_error_result(self, storage):
        tools = {tool.name: tool for tool in ObjectStorageToolset(f"file://{storage}").airflow_tools()}

        result = asyncio.run(tools["read_file"].call({"path": "../secrets/key.txt"}))

        assert result.is_error
        assert "leaves the storage root" in result.content


def test_a_file_the_connection_may_not_read_is_refused_not_fatal(storage):
    with patch("airflow.providers.common.ai.toolsets.object_storage.read_bytes", autospec=True) as read:
        read.side_effect = PermissionError("AccessDenied")
        with pytest.raises(ToolFailed, match="cannot be read: PermissionError"):
            _call(ObjectStorageToolset(f"file://{storage}"), "read_file", {"path": "orders.csv"})
