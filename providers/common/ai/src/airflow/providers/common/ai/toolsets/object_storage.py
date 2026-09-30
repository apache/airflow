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
"""Read-only toolset giving an agent the files under one object-storage path."""

from __future__ import annotations

import lzma
import os
import zlib
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Literal

from fsspec.implementations.local import LocalFileSystem
from pydantic_ai.exceptions import ToolFailed
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import ToolsetTool

from airflow.providers.common.ai.exceptions import LLMFileAnalysisError, LLMFileAnalysisLimitExceededError
from airflow.providers.common.ai.sandbox.output import format_size, render_file_window
from airflow.providers.common.ai.utils.file_analysis import (
    detect_compression,
    detect_file_format,
    read_bytes,
    sample_columnar_file,
)
from airflow.providers.common.ai.utils.masking import mask_secrets
from airflow.providers.common.ai.utils.tool_definition import (
    build_args_validator,
    return_schema_kwargs,
    serialize_for_llm,
)
from airflow.providers.common.ai.utils.toolset_base import AirflowToolset
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, ObjectStoragePath

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pydantic_ai._run_context import RunContext

LIST_FILES = "list_files"
GET_FILE_INFO = "get_file_info"
READ_FILE = "read_file"

_PATH_DESCRIPTION = "Path relative to the storage root, using / between parts. Omit for the root itself."

_SCHEMAS: dict[str, dict[str, Any]] = {
    LIST_FILES: {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": _PATH_DESCRIPTION},
            "offset": {
                "type": ["integer", "null"],
                "description": "Entry to start listing from (0-indexed), to page through a large directory.",
            },
        },
        "required": [],
    },
    GET_FILE_INFO: {
        "type": "object",
        "properties": {"path": {"type": "string", "description": _PATH_DESCRIPTION}},
        "required": ["path"],
    },
    READ_FILE: {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": _PATH_DESCRIPTION},
            "offset": {
                "type": ["integer", "null"],
                "description": "Line number to start reading from (1-indexed).",
            },
            "limit": {"type": ["integer", "null"], "description": "Maximum number of lines to read."},
        },
        "required": ["path"],
    },
}

_DESCRIPTIONS = {
    LIST_FILES: (
        "List the files and directories in one directory of the storage, sorted by name. "
        "Directories end with a slash; list one of them to go deeper. A large directory is listed "
        "a page at a time, and the result tells you the offset to continue from."
    ),
    GET_FILE_INFO: "Get the size and last-modified time of one file or directory.",
    READ_FILE: (
        "Read a text file. Long files are returned a window at a time and the result tells you the "
        "offset to continue from. For a Parquet or Avro file, returns its row count, schema and first "
        "rows; offset and limit do not apply to it."
    ),
}

_MAX_OUTPUT_LINES = 2000
_SAMPLE_ROWS = 20
_COLUMNAR_FORMATS: tuple[Literal["parquet", "avro"], ...] = ("parquet", "avro")
_MEDIA_FORMATS = frozenset({"jpeg", "jpg", "pdf", "png"})


class ObjectStorageToolset(AirflowToolset):
    """
    Give an agent read-only access to the files under one object-storage path.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Exposes three tools, ``list_files``, ``get_file_info`` and ``read_file``, rooted at
    ``path``, which is any location Airflow's
    :class:`~airflow.sdk.ObjectStoragePath` can open: ``s3://``, ``gs://``, ``abfs://``,
    ``file://`` and the rest, with credentials from ``conn_id``. The model names files by
    paths relative to that root. It cannot write, delete or move anything, and a path that
    is absolute, carries a scheme, or climbs out of the root with ``..`` is refused.

    ``read_file`` returns a text file a window of lines at a time, like the sandbox's own
    ``read_file``, and a Parquet or Avro file as its row count, schema and first rows. Compressed text
    (``.gz``, ``.bz2``, ``.xz``) is decompressed. Images, PDFs and other binary files are
    refused, as is any file larger than ``max_read_bytes``. So is a file that cannot be read,
    such as a corrupt one, or one the connection may not open: the model is told why, and the
    run goes on. On a local root, a symlink that leads out of the root is refused too.

    :param path: Root the agent may read under. Templated when the toolset is passed to
        ``AgentOperator`` / ``@task.agent``.
    :param conn_id: Airflow connection for the storage, or ``None`` for the default
        credentials of its protocol. Templated like ``path``.
    :param max_files: Most entries one ``list_files`` result holds; the model pages through
        a larger directory. The whole directory is still listed from storage on each call.
        Default ``200``.
    :param max_read_bytes: Largest file ``read_file`` will open, after decompression.
        Default 10 MiB.
    :param max_output_bytes: Most bytes one ``read_file`` result holds; the model reads on
        from the offset it is given. Default 50 KiB.
    :param tool_prefix: Prefix for the three tool names, e.g. ``"reports"`` gives
        ``reports_read_file``. Set this when one agent has another toolset with the same tool
        names, such as a second ``ObjectStorageToolset`` or a ``SandboxToolset``, whose
        ``read_file`` would collide, since duplicate tool names are rejected.
    """

    # Rendered, on a copy, by AgentOperator. Deliberately not ``template_fields``, which
    # Airflow's templater would render in place wherever the toolset is nested.
    agent_template_fields: Sequence[str] = ("_path", "_conn_id")

    def __init__(
        self,
        path: str,
        *,
        conn_id: str | None = None,
        max_files: int = 200,
        max_read_bytes: int = 10 * 1024 * 1024,
        max_output_bytes: int = 50 * 1024,
        tool_prefix: str = "",
    ) -> None:
        for name, value in (
            ("max_files", max_files),
            ("max_read_bytes", max_read_bytes),
            ("max_output_bytes", max_output_bytes),
        ):
            if value < 1:
                raise ValueError(f"{name} must be at least 1, got {value}.")
        if tool_prefix and not tool_prefix.isidentifier():
            raise ValueError(f"tool_prefix must be a valid Python identifier, got {tool_prefix!r}.")
        self._path = path
        self._conn_id = conn_id
        self._max_files = max_files
        self._max_read_bytes = max_read_bytes
        self._max_output_bytes = max_output_bytes
        self._tool_prefix = tool_prefix

    @property
    def id(self) -> str:
        suffix = f"-{self._tool_prefix}" if self._tool_prefix else ""
        return f"object-storage-{self._conn_id or 'default'}{suffix}"

    def _tool_name(self, base: str) -> str:
        return f"{self._tool_prefix}_{base}" if self._tool_prefix else base

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}
        for base, schema in _SCHEMAS.items():
            name = self._tool_name(base)
            tools[name] = ToolsetTool(
                toolset=self,
                tool_def=ToolDefinition(
                    name=name,
                    description=_DESCRIPTIONS[base],
                    parameters_json_schema=schema,
                    **return_schema_kwargs({"type": "string"}),
                ),
                max_retries=1,
                args_validator=build_args_validator(schema),
            )
        return tools

    async def execute_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        *,
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> str:
        base = name.removeprefix(f"{self._tool_prefix}_") if self._tool_prefix else name
        relative = tool_args.get("path") or ""
        try:
            if base == LIST_FILES:
                return await self.run_blocking(
                    self._list_files, relative, offset=tool_args.get("offset") or 0
                )
            if base == GET_FILE_INFO:
                return await self.run_blocking(self._get_file_info, relative)
            if base == READ_FILE:
                return await self.run_blocking(
                    self._read_file, relative, offset=tool_args.get("offset"), limit=tool_args.get("limit")
                )
        except ToolFailed:
            raise
        except (
            OSError,
            EOFError,
            ValueError,
            zlib.error,
            lzma.LZMAError,
            AirflowOptionalProviderFeatureException,
        ) as e:
            # Storage the connection may not read, a corrupt or mislabelled file, a codec this
            # Python build lacks: final for this path, so the model is told, not the task failed.
            raise ToolFailed(f"{relative or '/'!r} cannot be read: {type(e).__name__}: {e}") from None
        raise ValueError(f"Unknown tool: {name!r}")

    # ------------------------------------------------------------------
    # Tool implementations. Each runs in a worker thread.
    # ------------------------------------------------------------------

    def _root(self) -> ObjectStoragePath:
        # Built on use, not in __init__, so AgentOperator's rendering of _path and _conn_id applies.
        return ObjectStoragePath(self._path, conn_id=self._conn_id)

    def _resolve(self, relative: str) -> ObjectStoragePath:
        """Turn the model's relative path into a path under the root, or refuse it."""
        if relative.startswith("/") or "://" in relative:
            raise ToolFailed(f"{relative!r} is not a relative path. Name files relative to the storage root.")
        parts = [part for part in PurePosixPath(relative).parts if part != "."]
        if ".." in parts:
            raise ToolFailed(f"{relative!r} leaves the storage root; '..' is not allowed.")
        root = self._root()
        target = root.joinpath(*parts) if parts else root
        if not _inside_local_root(root, target.path):
            raise ToolFailed(f"{relative!r} resolves outside the storage root.")
        return target

    def _list_files(self, relative: str, *, offset: int) -> str:
        directory = self._resolve(relative)
        if not directory.is_dir():
            raise ToolFailed(f"{relative or '/'!r} is not a directory.")
        root = self._root()
        # One listing call with details, rather than a stat per entry, which on S3 or GCS is a
        # request each. Some stores list a directory's own placeholder object; skip it.
        own_path = directory.path.rstrip("/")
        entries = sorted(
            (
                info
                for info in directory.fs.ls(directory.path, detail=True)
                if info["name"].rstrip("/") != own_path and _inside_local_root(root, info["name"])
            ),
            key=_entry_name,
        )
        page = entries[offset : offset + self._max_files]
        listed = [
            {"name": f"{_entry_name(info)}/"}
            if info["type"] == "directory"
            else {"name": _entry_name(info), "size_bytes": info.get("size")}
            for info in page
        ]
        result: dict[str, Any] = {"path": relative or "/", "entries": listed}
        if offset + len(page) < len(entries):
            result["note"] = (
                f"Showing entries {offset + 1} to {offset + len(page)} of {len(entries)}; list again "
                f"with offset={offset + len(page)} for more."
            )
        return serialize_for_llm(result)

    def _get_file_info(self, relative: str) -> str:
        target = self._resolve(relative)
        # One request: on S3 or GCS, exists(), is_dir() and stat() would each be a round trip.
        try:
            stat = target.stat()
        except FileNotFoundError:
            raise ToolFailed(f"{relative!r} does not exist.") from None
        if stat.get("type") == "directory":
            return serialize_for_llm({"path": relative, "type": "directory"})
        info: dict[str, Any] = {"path": relative, "type": "file", "size_bytes": stat.st_size}
        if modified := _as_iso(stat.st_mtime):
            info["modified"] = modified
        return serialize_for_llm(info)

    def _read_file(self, relative: str, *, offset: int | None, limit: int | None) -> str:
        target = self._resolve(relative)
        if not target.is_file():
            raise ToolFailed(f"{relative!r} is not a file.")
        try:
            file_format, compression = detect_file_format(target)
        except LLMFileAnalysisError:
            # An extension file analysis does not know, such as .sql or .yaml.gz: read it as text.
            file_format, compression = "txt", detect_compression(target)
        if file_format in _MEDIA_FORMATS:
            raise ToolFailed(f"{relative!r} is a {file_format} file, which this tool cannot read as text.")
        try:
            for columnar in _COLUMNAR_FORMATS:
                if file_format == columnar:
                    sample = sample_columnar_file(
                        target, file_format=columnar, sample_rows=_SAMPLE_ROWS, max_bytes=self._max_read_bytes
                    )
                    # First, so that cutting a long sample never drops it.
                    shown = min(_SAMPLE_ROWS, sample.total_rows)
                    header = (
                        f"Rows: {sample.total_rows}. The schema and the first {shown} rows follow; "
                        f"offset and limit do not apply to {columnar.capitalize()} files.\n"
                    )
                    return _cut(header + sample.text, self._max_output_bytes)
            data = read_bytes(target, compression=compression, max_bytes=self._max_read_bytes)
        except LLMFileAnalysisLimitExceededError:
            raise ToolFailed(
                f"{relative!r} is larger than the {format_size(self._max_read_bytes)} this tool reads."
            ) from None
        if b"\x00" in data:
            raise ToolFailed(f"{relative!r} is a binary file, which this tool cannot read as text.")
        # Masked whole, before a window is cut: a secret spanning lines would otherwise leak a
        # line at a time.
        return render_file_window(
            mask_secrets(data),
            offset=offset,
            limit=limit,
            max_lines=_MAX_OUTPUT_LINES,
            max_bytes=self._max_output_bytes,
            long_line_hint="It cannot be read a line at a time.",
        )


def _entry_name(info: dict[str, Any]) -> str:
    return PurePosixPath(info["name"].rstrip("/")).name


def _inside_local_root(root: ObjectStoragePath, path: str) -> bool:
    """
    Whether ``path`` stays inside ``root`` once symlinks are resolved, for a root on local disk.

    Checked by filesystem, not scheme: a root written as a plain path has no scheme but is
    still on the worker's disk, where a symlink under it could point anywhere. Object stores
    have no symlinks, so any other root is not checked here.
    """
    if not isinstance(root.fs, LocalFileSystem):
        return True
    real_root = os.path.realpath(root.path)
    return os.path.commonpath([real_root, os.path.realpath(path)]) == real_root


def _as_iso(modified: Any) -> str | None:
    """Return a modification time as ISO 8601; stores report a timestamp, a datetime or nothing."""
    if isinstance(modified, datetime):
        return modified.isoformat()
    if isinstance(modified, (int, float)) and modified > 0:
        return datetime.fromtimestamp(modified, tz=timezone.utc).isoformat()
    return None


def _cut(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    kept = encoded[:max_bytes].decode("utf-8", "ignore")
    return f"{kept}\n[... cut at {format_size(max_bytes)}]"
