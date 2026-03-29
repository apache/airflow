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
"""Curated multimodal toolset for file and object-store inspection."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema

from airflow.providers.common.ai.utils.file_analysis import (
    _infer_partitions,
    _resolve_paths,
    build_file_analysis_request,
    detect_file_format,
)
from airflow.providers.common.compat.sdk import ObjectStoragePath

if TYPE_CHECKING:
    from pydantic_ai._run_context import RunContext

_PASSTHROUGH_VALIDATOR = SchemaValidator(core_schema.any_schema())

_LIST_FILES_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "file_path": {
            "type": "string",
            "description": "Optional file or prefix to inspect. Defaults to the configured file_path.",
        },
    },
}

_LOAD_FILES_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "file_path": {
            "type": "string",
            "description": "Optional file or prefix to inspect. Defaults to the configured file_path.",
        },
    },
}


class MultimodalToolset(AbstractToolset[Any]):
    """
    Curated toolset that lets an LLM agent inspect files from a configured file path or prefix.

    ``list_files`` returns JSON metadata for files under ``file_path`` or an
    overridden tool argument. ``load_files`` returns normalized text context for
    text-like inputs and text plus ``BinaryContent`` attachments for images and
    PDFs. The toolset reuses the same safety limits and format handling as
    :class:`~airflow.providers.common.ai.operators.llm_file_analysis.LLMFileAnalysisOperator`.

    :param file_path: File, directory, or object-storage prefix to expose.
    :param file_conn_id: Optional Airflow connection ID for the storage backend.
    :param max_files: Maximum number of files to resolve from a directory or
        prefix. Default ``20``.
    :param max_file_size_bytes: Maximum size of any single input file. Default
        ``5 MiB``.
    :param max_total_size_bytes: Maximum cumulative size across all resolved
        files. Default ``20 MiB``.
    :param max_text_chars: Maximum normalized text context returned from
        ``load_files``. Default ``100000``.
    :param sample_rows: Maximum sampled rows or records for structured file
        previews. Default ``10``.
    """

    def __init__(
        self,
        file_path: str,
        *,
        file_conn_id: str | None = None,
        max_files: int = 20,
        max_file_size_bytes: int = 5 * 1024 * 1024,
        max_total_size_bytes: int = 20 * 1024 * 1024,
        max_text_chars: int = 100_000,
        sample_rows: int = 10,
    ) -> None:
        if max_files <= 0:
            raise ValueError("max_files must be greater than zero.")
        if max_file_size_bytes <= 0:
            raise ValueError("max_file_size_bytes must be greater than zero.")
        if max_total_size_bytes <= 0:
            raise ValueError("max_total_size_bytes must be greater than zero.")
        if max_text_chars <= 0:
            raise ValueError("max_text_chars must be greater than zero.")
        if sample_rows <= 0:
            raise ValueError("sample_rows must be greater than zero.")

        self._file_path = file_path
        self._file_conn_id = file_conn_id
        self._max_files = max_files
        self._max_file_size_bytes = max_file_size_bytes
        self._max_total_size_bytes = max_total_size_bytes
        self._max_text_chars = max_text_chars
        self._sample_rows = sample_rows

    @property
    def id(self) -> str:
        return f"multimodal-{self._file_path}"

    def _resolve_target(self, file_path: str = "") -> ObjectStoragePath:
        return ObjectStoragePath(file_path or self._file_path, conn_id=self._file_conn_id)

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}
        for name, description, schema in (
            (
                "list_files",
                "List files and metadata for the configured file_path or an overridden file_path.",
                _LIST_FILES_SCHEMA,
            ),
            (
                "load_files",
                "Load normalized text context and multimodal attachments for the configured file_path or an overridden file_path.",
                _LOAD_FILES_SCHEMA,
            ),
        ):
            tool_def = ToolDefinition(
                name=name,
                description=description,
                parameters_json_schema=schema,
                sequential=True,
            )
            tools[name] = ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=1,
                args_validator=_PASSTHROUGH_VALIDATOR,
            )
        return tools

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        if name == "list_files":
            return self._list_files(tool_args.get("file_path", ""))
        if name == "load_files":
            return self._load_files(tool_args.get("file_path", ""))
        raise ValueError(f"Unknown tool: {name!r}")

    def _list_files(self, file_path: str = "") -> str:
        target = self._resolve_target(file_path)
        resolved_paths, omitted_files = _resolve_paths(root=target, max_files=self._max_files)

        files: list[dict[str, Any]] = []
        for path in resolved_paths:
            file_format, compression = detect_file_format(path)
            files.append(
                {
                    "path": str(path),
                    "format": file_format,
                    "size_bytes": path.stat().st_size,
                    "compression": compression,
                    "partitions": list(_infer_partitions(path)),
                }
            )

        return json.dumps(
            {
                "file_path": file_path or self._file_path,
                "files": files,
                "omitted_files": omitted_files,
            }
        )

    def _load_files(self, file_path: str = "") -> Any:
        target = self._resolve_target(file_path)
        request = build_file_analysis_request(
            file_path=str(target),
            file_conn_id=self._file_conn_id,
            prompt="",
            include_user_request=False,
            multi_modal=True,
            max_files=self._max_files,
            max_file_size_bytes=self._max_file_size_bytes,
            max_total_size_bytes=self._max_total_size_bytes,
            max_text_chars=self._max_text_chars,
            sample_rows=self._sample_rows,
        )
        return request.user_content
