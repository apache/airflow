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
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic_ai._run_context import RunContext
from pydantic_ai.messages import BinaryContent
from pydantic_ai.toolsets.abstract import ToolsetTool

from airflow.providers.common.ai.exceptions import LLMFileAnalysisLimitExceededError
from airflow.providers.common.ai.toolsets.multimodal import MultimodalToolset

AIRFLOW_PNG_PATH = Path(__file__).resolve().parents[1] / "assets" / "airflow-3-task-sdk.png"


def _mock_ctx() -> MagicMock:
    return MagicMock(spec=RunContext)


def _mock_tool() -> MagicMock:
    return MagicMock(spec=ToolsetTool)


class TestMultimodalToolsetInit:
    def test_rejects_non_positive_limits(self):
        with pytest.raises(ValueError, match="max_files"):
            MultimodalToolset("/tmp", max_files=0)

        with pytest.raises(ValueError, match="sample_rows"):
            MultimodalToolset("/tmp", sample_rows=0)

    def test_id_includes_file_path(self):
        ts = MultimodalToolset("/tmp/example")
        assert ts.id == "multimodal-/tmp/example"


class TestMultimodalToolsetGetTools:
    def test_returns_expected_tools(self):
        ts = MultimodalToolset("/tmp/example")
        tools = asyncio.run(ts.get_tools(ctx=_mock_ctx()))

        assert set(tools.keys()) == {"list_files", "load_files"}
        assert all(tool.tool_def.sequential is True for tool in tools.values())


class TestMultimodalToolsetListFiles:
    def test_returns_file_metadata(self, tmp_path):
        (tmp_path / "a.log").write_text("a", encoding="utf-8")
        nested_dir = tmp_path / "nested"
        nested_dir.mkdir()
        (nested_dir / "b.json").write_text('{"status": "ok"}', encoding="utf-8")

        ts = MultimodalToolset(str(tmp_path))
        result = asyncio.run(ts.call_tool("list_files", {}, ctx=_mock_ctx(), tool=_mock_tool()))

        payload = json.loads(result)
        assert payload["file_path"] == str(tmp_path)
        assert [entry["path"] for entry in payload["files"]] == [
            str(tmp_path / "a.log"),
            str(nested_dir / "b.json"),
        ]
        assert payload["files"][0]["format"] == "log"
        assert payload["files"][1]["format"] == "json"

    def test_reports_omitted_files(self, tmp_path):
        (tmp_path / "a.log").write_text("a", encoding="utf-8")
        (tmp_path / "b.log").write_text("b", encoding="utf-8")

        ts = MultimodalToolset(str(tmp_path), max_files=1)
        result = asyncio.run(ts.call_tool("list_files", {}, ctx=_mock_ctx(), tool=_mock_tool()))

        payload = json.loads(result)
        assert len(payload["files"]) == 1
        assert payload["omitted_files"] == 1

    def test_tool_argument_overrides_configured_file_path(self, tmp_path):
        nested_dir = tmp_path / "nested"
        nested_dir.mkdir()
        (nested_dir / "b.json").write_text('{"status": "ok"}', encoding="utf-8")

        ts = MultimodalToolset(str(tmp_path / "a.log"))
        result = asyncio.run(
            ts.call_tool("list_files", {"file_path": str(nested_dir)}, ctx=_mock_ctx(), tool=_mock_tool())
        )

        payload = json.loads(result)
        assert payload["file_path"] == str(nested_dir)
        assert payload["files"] == [
            {
                "path": str(nested_dir / "b.json"),
                "format": "json",
                "size_bytes": (nested_dir / "b.json").stat().st_size,
                "compression": None,
                "partitions": [],
            }
        ]

    def test_lists_oversized_file_metadata_without_loading_content(self, tmp_path):
        large_file = tmp_path / "large.log"
        large_file.write_text("x" * 32, encoding="utf-8")

        ts = MultimodalToolset(str(tmp_path), max_file_size_bytes=8)
        result = asyncio.run(ts.call_tool("list_files", {}, ctx=_mock_ctx(), tool=_mock_tool()))

        payload = json.loads(result)
        assert payload["files"] == [
            {
                "path": str(large_file),
                "format": "log",
                "size_bytes": 32,
                "compression": None,
                "partitions": [],
            }
        ]


class TestMultimodalToolsetLoadFiles:
    def test_returns_prompt_free_text_context(self, tmp_path):
        (tmp_path / "app.log").write_text("first line\nsecond line\n", encoding="utf-8")

        ts = MultimodalToolset(str(tmp_path))
        result = asyncio.run(
            ts.call_tool(
                "load_files",
                {"file_path": str(tmp_path / "app.log")},
                ctx=_mock_ctx(),
                tool=_mock_tool(),
            )
        )

        assert isinstance(result, str)
        assert "Resolved files:" in result
        assert "first line" in result
        assert "User request:" not in result

    def test_accepts_absolute_file_path_within_root(self, tmp_path):
        path = tmp_path / "app.log"
        path.write_text("first line\nsecond line\n", encoding="utf-8")

        ts = MultimodalToolset(str(tmp_path))
        result = asyncio.run(
            ts.call_tool("load_files", {"file_path": str(path)}, ctx=_mock_ctx(), tool=_mock_tool())
        )

        assert isinstance(result, str)
        assert "first line" in result

    def test_returns_binary_attachment_for_image(self):
        ts = MultimodalToolset(str(AIRFLOW_PNG_PATH))
        result = asyncio.run(ts.call_tool("load_files", {}, ctx=_mock_ctx(), tool=_mock_tool()))

        assert isinstance(result, list)
        assert isinstance(result[0], str)
        assert isinstance(result[1], BinaryContent)
        assert result[1].media_type == "image/png"

    def test_load_files_still_rejects_oversized_file(self, tmp_path):
        large_file = tmp_path / "large.log"
        large_file.write_text("x" * 32, encoding="utf-8")

        ts = MultimodalToolset(str(tmp_path), max_file_size_bytes=8)

        with pytest.raises(LLMFileAnalysisLimitExceededError, match="per-file limit"):
            asyncio.run(
                ts.call_tool(
                    "load_files",
                    {"file_path": str(large_file)},
                    ctx=_mock_ctx(),
                    tool=_mock_tool(),
                )
            )

    def test_invalid_subpath_under_file_path_surfaces_missing_path_error(self):
        ts = MultimodalToolset(str(AIRFLOW_PNG_PATH))

        with pytest.raises(FileNotFoundError, match="No files found"):
            asyncio.run(
                ts.call_tool(
                    "load_files",
                    {"file_path": f"{AIRFLOW_PNG_PATH}/child.png"},
                    ctx=_mock_ctx(),
                    tool=_mock_tool(),
                )
            )
