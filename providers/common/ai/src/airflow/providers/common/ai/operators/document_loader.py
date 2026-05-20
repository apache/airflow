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

import csv
import glob
import io
import json
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseOperator,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


# Type alias for path-like inputs that the parsers can read from. ``Path`` is
# the local filesystem; ``ObjectStoragePath`` covers ``s3://``, ``gs://``,
# ``azure://``, ``file://``, ... via fsspec. Both expose the methods we need
# (``read_bytes``, ``open``, ``name``, ``suffix``) so the parsers stay
# polymorphic.
FilePathT = Any  # Path | ObjectStoragePath


class DocumentLoaderOperator(BaseOperator):
    """
    Parse files into ``list[dict(text, metadata)]`` for downstream embedding.

    Bridges Airflow's connectivity layer (hooks that produce bytes or local
    files) and the AI embedding layer (operators that need structured text
    with metadata). Framework-agnostic: no LlamaIndex, LangChain, or other
    AI framework dependency.

    Built-in parsers handle ``.txt``, ``.md``, ``.csv``, and ``.json`` with
    zero extra dependencies. PDF and DOCX support require optional packages
    installable via extras::

        pip install apache-airflow-providers-common-ai[pdf]    # pypdf
        pip install apache-airflow-providers-common-ai[docx]   # python-docx

    Provide exactly one of ``source_path`` or ``source_bytes``. When using
    ``source_bytes``, ``file_type`` is required so the operator knows which
    parser to use.

    The operator is intentionally a **loader**: it does not split documents
    into fixed-size chunks. Pass the output to a downstream text-splitter or
    embedding operator if you need chunking.

    :param source_path: A local path, glob pattern, or storage URI
        (``s3://``, ``gs://``, ``azure://``, ``file://``, ...). Cloud URIs
        go through :class:`~airflow.sdk.ObjectStoragePath` / fsspec.
        ``**`` enables recursive matching for local globs. Cloud URIs
        accept a single file or a directory; cross-directory globs in a
        cloud URI are not supported in this version.
    :param source_conn_id: Airflow connection ID used by
        ``ObjectStoragePath`` for cloud URIs (``aws_default``,
        ``google_cloud_default``, ...). Ignored for local paths.
    :param source_bytes: Raw file bytes, typically from XCom.
    :param file_type: File extension hint when using ``source_bytes``
        (e.g. ``".pdf"``). Also accepted with ``source_path`` to override
        auto-detection.
    :param parser: Parsing backend selection. ``"auto"`` (default) picks the
        backend from the file extension.
    :param file_extensions: When ``source_path`` is a directory or glob,
        only process files whose extension is in this list. When omitted,
        the operator processes only files whose extension is known to the
        built-in dispatch (others are skipped with a warning) and silently
        ignores files whose name starts with a dot.
    :param metadata_fields: Extra key-value pairs merged into every
        document's ``metadata`` dict. Auto-extracted fields such as
        ``file_name``, ``file_path``, ``row_index``, ``item_index``, and
        ``page_number`` take precedence over keys with the same name.
    :param encoding: Text encoding used for ``.txt``/``.md``/``.csv``/``.json``
        and for the bytes path. Defaults to ``"utf-8"``.
    :param encoding_errors: How decode errors are handled. Defaults to
        ``"strict"``; set to ``"replace"`` or ``"ignore"`` to tolerate
        mixed-encoding inputs at the cost of some character loss.
    :param json_text_field: When parsing JSON, treat this key as the
        embedding text and put every other key into ``metadata``. Applies
        to each item when the top-level JSON is a list, or to the object
        when it is a single dict. When ``None`` (default), the operator
        flattens dicts into ``"k: v, k: v"`` text (same shape as the CSV
        parser).
    """

    template_fields: Sequence[str] = (
        "source_path",
        "source_conn_id",
        "file_type",
        "file_extensions",
        "parser",
        "metadata_fields",
    )

    EXTENSION_BACKEND_MAP: dict[str, str] = {
        ".txt": "text",
        ".md": "text",
        ".csv": "csv",
        ".json": "json",
        ".pdf": "pypdf",
        ".docx": "python-docx",
    }

    def __init__(
        self,
        *,
        source_path: str | None = None,
        source_conn_id: str | None = None,
        source_bytes: bytes | None = None,
        file_type: str | None = None,
        parser: str = "auto",
        file_extensions: list[str] | None = None,
        metadata_fields: dict[str, Any] | None = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        json_text_field: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if source_path is not None and source_bytes is not None:
            raise ValueError("Provide exactly one of 'source_path' or 'source_bytes', not both.")
        if source_path is None and source_bytes is None:
            raise ValueError("Provide exactly one of 'source_path' or 'source_bytes'.")
        if source_bytes is not None and file_type is None:
            raise ValueError("'file_type' is required when using 'source_bytes' (e.g. '.pdf').")

        self.source_path = source_path
        self.source_conn_id = source_conn_id
        self.source_bytes = source_bytes
        self.file_type = file_type
        self.parser = parser
        self.file_extensions = file_extensions
        self.metadata_fields = metadata_fields
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.json_text_field = json_text_field

    def execute(self, context: Context) -> list[dict[str, Any]]:
        if self.source_bytes is not None:
            assert self.file_type is not None  # noqa: S101 -- enforced in __init__
            documents = self._parse_bytes(self.source_bytes, self.file_type)
            file_count = 1
        else:
            assert self.source_path is not None  # noqa: S101 -- enforced in __init__
            files = self._resolve_files(self.source_path)
            if not files:
                raise FileNotFoundError(f"No files found matching '{self.source_path}'.")
            file_count = len(files)
            documents = []
            for file_path in files:
                ext = self.file_type or file_path.suffix.lower()
                parsed = self._parse_file(file_path, ext)
                for doc in parsed:
                    doc["metadata"]["file_name"] = file_path.name
                    doc["metadata"]["file_path"] = str(file_path)
                documents.extend(parsed)

        if self.metadata_fields:
            for doc in documents:
                # Auto-extracted keys (file_name, page_number, ...) take precedence.
                for key, value in self.metadata_fields.items():
                    doc["metadata"].setdefault(key, value)

        self.log.info("Parsed %d documents from %d file(s)", len(documents), file_count)
        return documents

    def _resolve_files(self, source_path: str) -> list[FilePathT]:
        # A storage URI (``s3://``, ``gs://``, ``file://``, ...) goes through
        # ObjectStoragePath / fsspec; a bare local path keeps the existing
        # glob behaviour. The heuristic is intentionally simple: presence of
        # ``://`` indicates a URI.
        if "://" in source_path:
            return self._resolve_remote_files(source_path)
        return self._resolve_local_files(source_path)

    def _resolve_local_files(self, source_path: str) -> list[Path]:
        path = Path(source_path)
        if path.is_file():
            return [path]

        if path.is_dir():
            candidates = sorted(p for p in path.iterdir() if not p.name.startswith("."))
            is_directory_mode = True
        else:
            # `recursive=True` makes `**` match across directories per the docstring.
            candidates = [Path(p) for p in sorted(glob.glob(source_path, recursive=True))]
            is_directory_mode = False

        return self._filter_files([p for p in candidates if p.is_file()], is_directory_mode=is_directory_mode)

    def _resolve_remote_files(self, source_path: str) -> list[FilePathT]:
        from airflow.sdk import ObjectStoragePath

        root = ObjectStoragePath(source_path, conn_id=self.source_conn_id)
        try:
            if root.is_file():
                return [root]
        except FileNotFoundError:
            # Some fsspec backends raise instead of returning False.
            pass

        if not root.is_dir():
            raise FileNotFoundError(
                f"Cloud URI '{source_path}' is neither a file nor a directory. "
                "Cross-directory globs in cloud URIs aren't supported here; "
                "point ``source_path`` at a single object or a directory."
            )

        candidates = sorted(
            (p for p in root.iterdir() if not p.name.startswith(".")),
            key=str,
        )
        return self._filter_files([p for p in candidates if p.is_file()], is_directory_mode=True)

    def _filter_files(self, results: list[FilePathT], *, is_directory_mode: bool) -> list[FilePathT]:
        if self.file_extensions:
            allowed = {(ext if ext.startswith(".") else f".{ext}").lower() for ext in self.file_extensions}
            return [p for p in results if p.suffix.lower() in allowed]

        if is_directory_mode:
            # No explicit filter in directory mode: skip files we don't know
            # how to parse rather than crashing on the first stray file
            # (``.DS_Store``, editor swap files, etc.). A glob is treated as
            # intentional and parsed via the explicit ``parser`` argument.
            known = set(self.EXTENSION_BACKEND_MAP.keys())
            unknown = [p for p in results if p.suffix.lower() not in known]
            if unknown:
                self.log.warning(
                    "Skipping %d file(s) with unrecognised extension: %s",
                    len(unknown),
                    ", ".join(sorted({p.suffix or "<no ext>" for p in unknown})),
                )
            return [p for p in results if p.suffix.lower() in known]

        return results

    def _parse_bytes(self, raw: bytes, file_type: str) -> list[dict[str, Any]]:
        ext = file_type if file_type.startswith(".") else f".{file_type}"
        backend = self._resolve_backend(ext)

        if backend == "pypdf":
            return self._parse_pdf_stream(io.BytesIO(raw))
        if backend == "python-docx":
            return self._parse_docx_stream(io.BytesIO(raw))

        text = self._decode(raw, source_hint=f"<bytes:{ext}>")
        if backend == "csv":
            return self._parse_csv_text(text)
        if backend == "json":
            return self._parse_json_text(text)
        return [{"text": text, "metadata": {}}]

    def _parse_file(self, file_path: Path, ext: str) -> list[dict[str, Any]]:
        backend = self._resolve_backend(ext)

        if backend == "text":
            return self._parse_text(file_path)
        if backend == "csv":
            return self._parse_csv(file_path)
        if backend == "json":
            return self._parse_json(file_path)
        if backend == "pypdf":
            with file_path.open("rb") as fh:
                return self._parse_pdf_stream(fh)
        if backend == "python-docx":
            with file_path.open("rb") as fh:
                return self._parse_docx_stream(fh)

        raise ValueError(f"No parser found for backend '{backend}'.")

    def _resolve_backend(self, ext: str) -> str:
        if self.parser != "auto":
            return self.parser

        ext = ext.lower()
        if ext not in self.EXTENSION_BACKEND_MAP:
            supported = ", ".join(sorted(self.EXTENSION_BACKEND_MAP.keys()))
            raise ValueError(
                f"No parser registered for extension '{ext}'. "
                f"Supported extensions: {supported}. "
                f"Set 'parser' explicitly to override auto-detection."
            )
        return self.EXTENSION_BACKEND_MAP[ext]

    def _decode(self, raw: bytes, *, source_hint: str) -> str:
        try:
            return raw.decode(self.encoding, errors=self.encoding_errors)
        except UnicodeDecodeError as e:
            raise ValueError(
                f"Failed to decode {source_hint} as {self.encoding!r}: {e}. "
                f"Pass encoding=... or encoding_errors='replace' to tolerate this."
            ) from e

    def _read_text(self, file_path: Path) -> str:
        return self._decode(file_path.read_bytes(), source_hint=str(file_path))

    def _parse_text(self, file_path: Path) -> list[dict[str, Any]]:
        return [{"text": self._read_text(file_path), "metadata": {}}]

    def _parse_csv(self, file_path: Path) -> list[dict[str, Any]]:
        return self._parse_csv_text(self._read_text(file_path))

    def _parse_csv_text(self, text: str) -> list[dict[str, Any]]:
        reader = csv.DictReader(io.StringIO(text))
        documents = []
        for row_idx, row in enumerate(reader):
            # Skip empty cells to avoid noisy "col: ," in the text.
            row_text = ", ".join(f"{k}: {v}" for k, v in row.items() if v != "")
            documents.append({"text": row_text, "metadata": {"row_index": row_idx}})
        return documents

    def _parse_json(self, file_path: Path) -> list[dict[str, Any]]:
        return self._parse_json_text(self._read_text(file_path))

    def _parse_json_text(self, text: str) -> list[dict[str, Any]]:
        data = json.loads(text)
        if isinstance(data, list):
            return [self._json_item_to_doc(item, item_index=idx) for idx, item in enumerate(data)]
        return [self._json_item_to_doc(data, item_index=None)]

    def _json_item_to_doc(self, item: Any, *, item_index: int | None) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        if item_index is not None:
            metadata["item_index"] = item_index

        if isinstance(item, str):
            text = item
        elif isinstance(item, dict):
            if self.json_text_field is not None:
                # Pull the named field out as the text; everything else goes
                # to metadata. Common pattern for "ingest article body, keep
                # title/author/url for filtering".
                text_value = item.get(self.json_text_field, "")
                text = (
                    text_value if isinstance(text_value, str) else json.dumps(text_value, ensure_ascii=False)
                )
                for k, v in item.items():
                    if k == self.json_text_field:
                        continue
                    metadata[k] = v
            else:
                # No text field declared: flatten dict to "k: v, k: v" so the
                # embedding sees content tokens, not JSON syntax. Mirrors the
                # CSV parser's behaviour.
                text = ", ".join(f"{k}: {v}" for k, v in item.items() if v not in (None, ""))
        else:
            text = json.dumps(item, ensure_ascii=False)

        return {"text": text, "metadata": metadata}

    def _parse_pdf_stream(self, stream: BinaryIO) -> list[dict[str, Any]]:
        try:
            from pypdf import PdfReader
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)

        reader = PdfReader(stream)
        documents = []
        for page_num, page in enumerate(reader.pages):
            text = page.extract_text() or ""
            if text.strip():
                documents.append({"text": text, "metadata": {"page_number": page_num + 1}})
        return documents

    def _parse_docx_stream(self, stream: BinaryIO) -> list[dict[str, Any]]:
        """
        Parse a DOCX stream into documents.

        Extracts paragraph text only. Tables, headers, footers, and footnotes
        are not included. For richer DOCX parsing, plug in a dedicated
        extraction tool (``Unstructured``, ``docling``) as a custom parser
        backend.
        """
        try:
            from docx import Document
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)

        doc = Document(stream)
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        text = "\n\n".join(paragraphs)
        return [{"text": text, "metadata": {}}]
