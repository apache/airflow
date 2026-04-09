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
"""Helpers for building file-analysis prompts for LLM operators."""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
from bisect import insort
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

from pydantic_ai.messages import BinaryContent

from airflow.providers.common.ai.exceptions import (
    LLMFileAnalysisLimitExceededError,
    LLMFileAnalysisMultimodalRequiredError,
    LLMFileAnalysisUnsupportedFormatError,
)
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, ObjectStoragePath

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pydantic_ai.messages import UserContent

SUPPORTED_FILE_FORMATS: tuple[str, ...] = (
    "avro",
    "csv",
    "jpeg",
    "jpg",
    "json",
    "log",
    "parquet",
    "pdf",
    "png",
)

_TEXT_LIKE_FORMATS = frozenset({"csv", "json", "log", "avro", "parquet"})
_MULTI_MODAL_FORMATS = frozenset({"jpeg", "jpg", "pdf", "png"})
_COMPRESSION_SUFFIXES = {
    "bz2": "bzip2",
    "gz": "gzip",
    "snappy": "snappy",
    "xz": "xz",
    "zst": "zstd",
}
_GZIP_SUPPORTED_FORMATS = frozenset({"csv", "json", "log"})
_TEXT_SAMPLE_HEAD_CHARS = 8_000
_TEXT_SAMPLE_TAIL_CHARS = 2_000
_MEDIA_TYPES = {
    "jpeg": "image/jpeg",
    "jpg": "image/jpeg",
    "pdf": "application/pdf",
    "png": "image/png",
}
log = logging.getLogger(__name__)


@dataclass
class FileAnalysisRequest:
    """Prepared prompt content and discovery metadata for the file-analysis operator."""

    user_content: str | Sequence[UserContent]
    resolved_paths: list[str]
    total_size_bytes: int
    omitted_files: int = 0
    text_truncated: bool = False
    attachment_count: int = 0
    text_file_count: int = 0


@dataclass
class _PreparedFile:
    path: ObjectStoragePath
    file_format: str
    size_bytes: int
    compression: str | None
    partitions: tuple[str, ...]
    estimated_rows: int | None = None
    text_content: str | None = None
    attachment: BinaryContent | None = None
    content_size_bytes: int = 0
    content_truncated: bool = False
    content_omitted: bool = False


@dataclass
class _DiscoveredFile:
    path: ObjectStoragePath
    file_format: str
    size_bytes: int
    compression: str | None


@dataclass
class _RenderResult:
    text: str
    estimated_rows: int | None
    content_size_bytes: int


def build_file_analysis_request(
    *,
    file_path: str,
    file_conn_id: str | None,
    prompt: str,
    multi_modal: bool,
    max_files: int,
    max_file_size_bytes: int,
    max_total_size_bytes: int,
    max_text_chars: int,
    sample_rows: int,
) -> FileAnalysisRequest:
    """Resolve files, normalize supported formats, and build prompt content for an LLM run."""
    if sample_rows <= 0:
        raise ValueError("sample_rows must be greater than zero.")
    log.info(
        "Preparing file analysis request for path=%s, file_conn_id=%s, multi_modal=%s, "
        "max_files=%s, max_file_size_bytes=%s, max_total_size_bytes=%s, max_text_chars=%s, sample_rows=%s",
        file_path,
        file_conn_id,
        multi_modal,
        max_files,
        max_file_size_bytes,
        max_total_size_bytes,
        max_text_chars,
        sample_rows,
    )
    root = ObjectStoragePath(file_path, conn_id=file_conn_id)
    resolved_paths, omitted_files = _resolve_paths(root=root, max_files=max_files)
    log.info(
        "Resolved %s file(s) from %s%s",
        len(resolved_paths),
        file_path,
        f"; omitted {omitted_files} additional file(s) due to max_files limit" if omitted_files else "",
    )
    if log.isEnabledFor(logging.DEBUG):
        log.debug("Resolved file paths: %s", [str(path) for path in resolved_paths])

    discovered_files: list[_DiscoveredFile] = []
    total_size_bytes = 0
    for path in resolved_paths:
        discovered = _discover_file(
            path=path,
            max_file_size_bytes=max_file_size_bytes,
        )
        total_size_bytes += discovered.size_bytes
        if total_size_bytes > max_total_size_bytes:
            log.info(
                "Rejecting file set before content reads because cumulative size reached %s bytes (limit=%s bytes).",
                total_size_bytes,
                max_total_size_bytes,
            )
            raise LLMFileAnalysisLimitExceededError(
                "Total input size exceeds the configured limit: "
                f"{total_size_bytes} bytes > {max_total_size_bytes} bytes."
            )
        discovered_files.append(discovered)

    log.info(
        "Validated byte limits for %s file(s) before reading file contents; total_size_bytes=%s.",
        len(discovered_files),
        total_size_bytes,
    )

    prepared_files: list[_PreparedFile] = []
    processed_size_bytes = 0
    for discovered in discovered_files:
        remaining_content_bytes = max_total_size_bytes - processed_size_bytes
        if remaining_content_bytes <= 0:
            raise LLMFileAnalysisLimitExceededError(
                "Total processed input size exceeds the configured limit after decompression."
            )
        prepared = _prepare_file(
            discovered_file=discovered,
            multi_modal=multi_modal,
            sample_rows=sample_rows,
            max_content_bytes=min(max_file_size_bytes, remaining_content_bytes),
        )
        processed_size_bytes += prepared.content_size_bytes
        prepared_files.append(prepared)

    text_truncated = _apply_text_budget(prepared_files=prepared_files, max_text_chars=max_text_chars)
    if text_truncated:
        log.info("Normalized text content exceeded max_text_chars=%s and was truncated.", max_text_chars)
    text_preamble = _build_text_preamble(
        prompt=prompt,
        prepared_files=prepared_files,
        omitted_files=omitted_files,
        text_truncated=text_truncated,
    )
    attachments = [prepared.attachment for prepared in prepared_files if prepared.attachment is not None]
    text_file_count = sum(1 for prepared in prepared_files if prepared.text_content is not None)
    user_content: str | list[UserContent]
    if attachments:
        user_content = [text_preamble, *attachments]
    else:
        user_content = text_preamble
    log.info(
        "Prepared file analysis request with %s text file(s), %s attachment(s), total_size_bytes=%s.",
        text_file_count,
        len(attachments),
        total_size_bytes,
    )
    if log.isEnabledFor(logging.DEBUG):
        log.debug("Prepared text preamble length=%s", len(text_preamble))
    return FileAnalysisRequest(
        user_content=user_content,
        resolved_paths=[str(path) for path in resolved_paths],
        total_size_bytes=total_size_bytes,
        omitted_files=omitted_files,
        text_truncated=text_truncated,
        attachment_count=len(attachments),
        text_file_count=text_file_count,
    )


def _resolve_paths(*, root: ObjectStoragePath, max_files: int) -> tuple[list[ObjectStoragePath], int]:
    try:
        if root.is_file():
            return [root], 0
    except FileNotFoundError:
        pass

    try:
        selected: list[tuple[str, ObjectStoragePath]] = []
        omitted_files = 0
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            path_key = str(path)
            if len(selected) < max_files:
                insort(selected, (path_key, path))
                continue
            if path_key < selected[-1][0]:
                insort(selected, (path_key, path))
                selected.pop()
            omitted_files += 1
    except (FileNotFoundError, NotADirectoryError):
        selected = []
        omitted_files = 0

    if not selected:
        raise FileNotFoundError(f"No files found for {root}.")

    return [path for _, path in selected], omitted_files


def _discover_file(*, path: ObjectStoragePath, max_file_size_bytes: int) -> _DiscoveredFile:
    file_format, compression = detect_file_format(path)
    size_bytes = path.stat().st_size
    log.debug(
        "Discovered file %s (format=%s, size_bytes=%s%s).",
        path,
        file_format,
        size_bytes,
        f", compression={compression}" if compression else "",
    )
    if size_bytes > max_file_size_bytes:
        log.info(
            "Rejecting file %s because size_bytes=%s exceeds the per-file limit=%s.",
            path,
            size_bytes,
            max_file_size_bytes,
        )
        raise LLMFileAnalysisLimitExceededError(
            f"File {path} exceeds the configured per-file limit: {size_bytes} bytes > {max_file_size_bytes} bytes."
        )
    return _DiscoveredFile(
        path=path,
        file_format=file_format,
        size_bytes=size_bytes,
        compression=compression,
    )


def _prepare_file(
    *,
    discovered_file: _DiscoveredFile,
    multi_modal: bool,
    sample_rows: int,
    max_content_bytes: int,
) -> _PreparedFile:
    path = discovered_file.path
    file_format = discovered_file.file_format
    size_bytes = discovered_file.size_bytes
    compression = discovered_file.compression
    log.debug(
        "Preparing file content for %s (format=%s, size_bytes=%s%s).",
        path,
        file_format,
        size_bytes,
        f", compression={compression}" if compression else "",
    )
    prepared = _PreparedFile(
        path=path,
        file_format=file_format,
        size_bytes=size_bytes,
        compression=compression,
        partitions=_infer_partitions(path),
    )

    if file_format in _MULTI_MODAL_FORMATS:
        if not multi_modal:
            log.info("Rejecting file %s because format=%s requires multi_modal=True.", path, file_format)
            raise LLMFileAnalysisMultimodalRequiredError(
                f"File {path} has format {file_format!r}; set multi_modal=True to analyze images or PDFs."
            )
        prepared.attachment = BinaryContent(
            data=_read_raw_bytes(path, compression=compression, max_bytes=max_content_bytes),
            media_type=_MEDIA_TYPES[file_format],
            identifier=str(path),
        )
        prepared.content_size_bytes = len(prepared.attachment.data)
        log.debug(
            "Attached %s as multimodal binary content with media_type=%s.", path, _MEDIA_TYPES[file_format]
        )
        return prepared

    render_result = _render_text_content(
        path=path,
        file_format=file_format,
        compression=compression,
        sample_rows=sample_rows,
        max_content_bytes=max_content_bytes,
    )
    prepared.text_content = render_result.text
    prepared.estimated_rows = render_result.estimated_rows
    prepared.content_size_bytes = render_result.content_size_bytes
    log.debug(
        "Normalized %s into text content of %s characters%s.",
        path,
        len(render_result.text),
        f"; estimated_rows={render_result.estimated_rows}"
        if render_result.estimated_rows is not None
        else "",
    )
    return prepared


def detect_file_format(path: ObjectStoragePath) -> tuple[str, str | None]:
    """Detect the logical file format and compression codec from a path suffix."""
    suffixes = [suffix.removeprefix(".").lower() for suffix in path.suffixes]
    compression: str | None = None
    if suffixes and suffixes[-1] in _COMPRESSION_SUFFIXES:
        compression = _COMPRESSION_SUFFIXES[suffixes[-1]]
        suffixes = suffixes[:-1]
    detected = suffixes[-1] if suffixes else "log"
    if detected not in SUPPORTED_FILE_FORMATS:
        raise LLMFileAnalysisUnsupportedFormatError(
            f"Unsupported file format {detected!r} for {path}. Supported formats: {', '.join(SUPPORTED_FILE_FORMATS)}."
        )
    if compression and compression != "gzip":
        log.info("Rejecting file %s because compression=%s is not supported.", path, compression)
        raise LLMFileAnalysisUnsupportedFormatError(
            f"Compression {compression!r} is not supported for file analysis."
        )
    if compression == "gzip" and detected not in _GZIP_SUPPORTED_FORMATS:
        raise LLMFileAnalysisUnsupportedFormatError(
            f"Compression {compression!r} is not supported for {detected!r} file analysis."
        )
    return detected, compression


def _render_text_content(
    *,
    path: ObjectStoragePath,
    file_format: str,
    compression: str | None,
    sample_rows: int,
    max_content_bytes: int,
) -> _RenderResult:
    if file_format == "json":
        return _render_json(path, compression=compression, max_content_bytes=max_content_bytes)
    if file_format == "csv":
        return _render_csv(
            path, compression=compression, sample_rows=sample_rows, max_content_bytes=max_content_bytes
        )
    if file_format == "parquet":
        return _render_parquet(path, sample_rows=sample_rows, max_content_bytes=max_content_bytes)
    if file_format == "avro":
        return _render_avro(path, sample_rows=sample_rows, max_content_bytes=max_content_bytes)
    return _render_text_like(path, compression=compression, max_content_bytes=max_content_bytes)


def _render_text_like(
    path: ObjectStoragePath, *, compression: str | None, max_content_bytes: int
) -> _RenderResult:
    raw_bytes = _read_raw_bytes(path, compression=compression, max_bytes=max_content_bytes)
    text = _decode_text(raw_bytes)
    return _RenderResult(text=_truncate_text(text), estimated_rows=None, content_size_bytes=len(raw_bytes))


def _render_json(
    path: ObjectStoragePath, *, compression: str | None, max_content_bytes: int
) -> _RenderResult:
    raw_bytes = _read_raw_bytes(path, compression=compression, max_bytes=max_content_bytes)
    decoded = _decode_text(raw_bytes)
    document = json.loads(decoded)
    if isinstance(document, list):
        estimated_rows = len(document)
    else:
        estimated_rows = None
    pretty = json.dumps(document, indent=2, sort_keys=True, default=str)
    return _RenderResult(
        text=_truncate_text(pretty),
        estimated_rows=estimated_rows,
        content_size_bytes=len(raw_bytes),
    )


def _render_csv(
    path: ObjectStoragePath, *, compression: str | None, sample_rows: int, max_content_bytes: int
) -> _RenderResult:
    raw_bytes = _read_raw_bytes(path, compression=compression, max_bytes=max_content_bytes)
    decoded = _decode_text(raw_bytes)
    reader = list(csv.reader(io.StringIO(decoded)))
    if not reader:
        return _RenderResult(text="", estimated_rows=0, content_size_bytes=len(raw_bytes))
    header, rows = reader[0], reader[1:]
    sampled_rows = rows[:sample_rows]
    payload = ["Header: " + ", ".join(header)]
    if sampled_rows:
        payload.append("Sample rows:")
        payload += [", ".join(str(value) for value in row) for row in sampled_rows]
    return _RenderResult(
        text=_truncate_text("\n".join(payload)),
        estimated_rows=len(rows),
        content_size_bytes=len(raw_bytes),
    )


def _render_parquet(path: ObjectStoragePath, *, sample_rows: int, max_content_bytes: int) -> _RenderResult:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise AirflowOptionalProviderFeatureException(
            "Parquet analysis requires the `parquet` extra for apache-airflow-providers-common-ai."
        ) from exc

    with path.open("rb") as handle:
        parquet_file = pq.ParquetFile(handle)
        metadata = parquet_file.metadata
        num_rows = metadata.num_rows if metadata is not None else 0

        handle.seek(0, io.SEEK_END)
        content_size_bytes = handle.tell()
        handle.seek(0)
        if content_size_bytes > max_content_bytes:
            raise LLMFileAnalysisLimitExceededError(
                f"File {path} exceeds the configured processed-content limit: {content_size_bytes} bytes > {max_content_bytes} bytes."
            )

        schema = ", ".join(f"{field.name}: {field.type}" for field in parquet_file.schema_arrow)
        sampled_rows: list[dict[str, Any]] = []
        if sample_rows > 0 and num_rows > 0 and parquet_file.num_row_groups > 0:
            remaining_rows = sample_rows
            for row_group_index in range(parquet_file.num_row_groups):
                if remaining_rows <= 0:
                    break
                row_group = parquet_file.read_row_group(row_group_index)
                if row_group.num_rows == 0:
                    continue
                group_rows = row_group.slice(0, remaining_rows).to_pylist()
                sampled_rows.extend(group_rows)
                remaining_rows -= len(group_rows)
    payload = [f"Schema: {schema}", "Sample rows:", json.dumps(sampled_rows, indent=2, default=str)]
    return _RenderResult(
        text=_truncate_text("\n".join(payload)),
        estimated_rows=num_rows,
        content_size_bytes=content_size_bytes,
    )


def _render_avro(path: ObjectStoragePath, *, sample_rows: int, max_content_bytes: int) -> _RenderResult:
    try:
        import fastavro
    except ImportError as exc:
        raise AirflowOptionalProviderFeatureException(
            "Avro analysis requires the `avro` extra for apache-airflow-providers-common-ai."
        ) from exc

    sampled_rows: list[dict[str, Any]] = []
    total_rows = 0
    with path.open("rb") as handle:
        handle.seek(0, io.SEEK_END)
        content_size_bytes = handle.tell()
        handle.seek(0)
        if content_size_bytes > max_content_bytes:
            raise LLMFileAnalysisLimitExceededError(
                f"File {path} exceeds the configured processed-content limit: {content_size_bytes} bytes > {max_content_bytes} bytes."
            )
        reader = fastavro.reader(handle)
        writer_schema = getattr(reader, "writer_schema", None)
        fully_read = False
        if sample_rows > 0:
            for record in reader:
                total_rows += 1
                if isinstance(record, dict):
                    sampled_rows.append({str(key): value for key, value in record.items()})
                if total_rows >= sample_rows:
                    break
            else:
                fully_read = True
    payload = [
        f"Schema: {json.dumps(writer_schema, indent=2, default=str)}",
        "Sample rows:",
        json.dumps(sampled_rows, indent=2, default=str),
    ]
    return _RenderResult(
        text=_truncate_text("\n".join(payload)),
        estimated_rows=total_rows if fully_read else None,
        content_size_bytes=content_size_bytes,
    )


def _read_raw_bytes(path: ObjectStoragePath, *, compression: str | None, max_bytes: int) -> bytes:
    with path.open("rb") as handle:
        if compression == "gzip":
            with gzip.GzipFile(fileobj=handle) as gzip_handle:
                return _read_limited_bytes(gzip_handle, path=path, max_bytes=max_bytes)
        return _read_limited_bytes(handle, path=path, max_bytes=max_bytes)


def _read_limited_bytes(handle: io.BufferedIOBase, *, path: ObjectStoragePath, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    total_bytes = 0
    while True:
        chunk = handle.read(min(64 * 1024, max_bytes - total_bytes + 1))
        if not chunk:
            break
        total_bytes += len(chunk)
        if total_bytes > max_bytes:
            raise LLMFileAnalysisLimitExceededError(
                f"File {path} exceeds the configured processed-content limit: > {max_bytes} bytes."
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _decode_text(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def _apply_text_budget(*, prepared_files: list[_PreparedFile], max_text_chars: int) -> bool:
    remaining = max_text_chars
    truncated_any = False
    for prepared in prepared_files:
        if prepared.text_content is None:
            continue
        if remaining <= 0:
            prepared.text_content = None
            prepared.content_omitted = True
            truncated_any = True
            log.debug(
                "Omitted normalized text for %s because the prompt text budget was exhausted.", prepared.path
            )
            continue
        original = prepared.text_content
        if len(original) > remaining:
            prepared.text_content = _truncate_text(original, max_chars=remaining)
            prepared.content_truncated = True
            truncated_any = True
            log.debug(
                "Truncated normalized text for %s from %s to %s characters to fit the remaining budget.",
                prepared.path,
                len(original),
                len(prepared.text_content),
            )
        remaining -= len(prepared.text_content)
    return truncated_any


def _build_text_preamble(
    *,
    prompt: str,
    prepared_files: list[_PreparedFile],
    omitted_files: int,
    text_truncated: bool,
) -> str:
    lines = [
        "User request:",
        prompt,
        "",
        "Resolved files:",
    ]
    text_sections: list[str] = []
    has_attachments = False
    for prepared in prepared_files:
        lines.append(f"- {_format_file_metadata(prepared)}")
        if prepared.text_content is not None:
            text_sections.append(f"### File: {prepared.path}\n{prepared.text_content}")
        if prepared.attachment is not None:
            has_attachments = True
    if omitted_files:
        lines.append(f"- omitted_files={omitted_files} (max_files limit reached)")
    if text_truncated:
        lines.append("- text_context_truncated=True")

    if text_sections:
        lines.extend(["", "Normalized content:", *text_sections])
    if has_attachments:
        lines.extend(
            [
                "",
                "Attached multimodal files follow this text block.",
                "Use the matching file metadata above when referring to those attachments.",
            ]
        )
    return "\n".join(lines)


def _truncate_text(text: str, *, max_chars: int = _TEXT_SAMPLE_HEAD_CHARS + _TEXT_SAMPLE_TAIL_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    if max_chars <= 32:
        return text[:max_chars]
    if max_chars >= _TEXT_SAMPLE_HEAD_CHARS + _TEXT_SAMPLE_TAIL_CHARS:
        head = _TEXT_SAMPLE_HEAD_CHARS
    else:
        head = max_chars // 2
    tail = max_chars - head - len("\n...\n")
    if tail <= 0:
        return text[:max_chars]
    return f"{text[:head]}\n...\n{text[-tail:]}"


def _format_file_metadata(prepared: _PreparedFile) -> str:
    metadata = [
        f"path={prepared.path}",
        f"format={prepared.file_format}",
        f"size_bytes={prepared.size_bytes}",
    ]
    if prepared.compression:
        metadata.append(f"compression={prepared.compression}")
    if prepared.estimated_rows is not None:
        metadata.append(f"estimated_rows={prepared.estimated_rows}")
    if prepared.partitions:
        metadata.append(f"partitions={list(prepared.partitions)}")
    if prepared.content_truncated:
        metadata.append("content_truncated=True")
    if prepared.content_omitted:
        metadata.append("content_omitted=True")
    if prepared.attachment is not None:
        metadata.append("attached_as_binary=True")
    return ", ".join(metadata)


def _infer_partitions(path: ObjectStoragePath) -> tuple[str, ...]:
    pure_path = PurePosixPath(path.path)
    return tuple(part for part in pure_path.parts if "=" in part)
