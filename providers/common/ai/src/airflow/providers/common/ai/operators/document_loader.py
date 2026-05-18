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
"""Operator for parsing files into document dicts suitable for embedding."""

from __future__ import annotations

import csv
import glob
import io
import json
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.sdk import Context


class DocumentLoaderOperator(BaseOperator):
    """
    Parse files into ``list[dict(text, metadata)]`` for downstream embedding.

    Bridges Airflow's connectivity layer (hooks that produce bytes or local
    files) and the AI embedding layer (operators that need structured text
    with metadata).  Framework-agnostic: no LlamaIndex, LangChain, or other
    AI framework dependency.

    Built-in parsers handle ``.txt``, ``.md``, ``.csv``, and ``.json`` with
    zero extra dependencies.  PDF and DOCX support require optional packages
    installable via extras::

        pip install apache-airflow-providers-common-ai[pdf]    # pypdf
        pip install apache-airflow-providers-common-ai[docx]   # python-docx

    Provide exactly one of ``source_path`` or ``source_bytes``.  When using
    ``source_bytes``, ``file_type`` is required so the operator knows which
    parser to use.

    :param source_path: Local file path or glob pattern (e.g. ``/data/*.pdf``).
    :param source_bytes: Raw file bytes, typically from XCom.
    :param file_type: File extension hint when using ``source_bytes``
        (e.g. ``".pdf"``).  Also accepted with ``source_path`` to override
        auto-detection.
    :param parser: Parsing backend selection.  ``"auto"`` (default) picks the
        backend from the file extension.
    :param file_extensions: When ``source_path`` is a directory or glob,
        only process files whose extension is in this list.
    :param metadata_fields: Extra key-value pairs merged into every
        document's ``metadata`` dict.
    """

    template_fields: Sequence[str] = (
        "source_path",
        "source_bytes",
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
        source_bytes: bytes | None = None,
        file_type: str | None = None,
        parser: str = "auto",
        file_extensions: list[str] | None = None,
        metadata_fields: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if source_path and source_bytes:
            raise ValueError("Provide exactly one of 'source_path' or 'source_bytes', not both.")
        if not source_path and not source_bytes:
            raise ValueError("Provide exactly one of 'source_path' or 'source_bytes'.")
        if source_bytes and not file_type:
            raise ValueError("'file_type' is required when using 'source_bytes' (e.g. '.pdf').")

        self.source_path = source_path
        self.source_bytes = source_bytes
        self.file_type = file_type
        self.parser = parser
        self.file_extensions = file_extensions
        self.metadata_fields = metadata_fields

    def execute(self, context: Context) -> list[dict[str, Any]]:
        if self.source_bytes:
            documents = self._parse_bytes(self.source_bytes, self.file_type)
            file_count = 1
        else:
            files = self._resolve_files(self.source_path)
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
                doc["metadata"].update(self.metadata_fields)

        self.log.info("Parsed %d documents from %d files", len(documents), file_count)
        return documents

    def _resolve_files(self, source_path: str) -> list[Path]:
        path = Path(source_path)
        if path.is_file():
            return [path]

        if path.is_dir():
            candidates = sorted(path.iterdir())
        else:
            candidates = [Path(p) for p in sorted(glob.glob(source_path))]

        results = [p for p in candidates if p.is_file()]

        if self.file_extensions:
            allowed = {ext if ext.startswith(".") else f".{ext}" for ext in self.file_extensions}
            results = [p for p in results if p.suffix.lower() in allowed]

        return results

    def _parse_bytes(self, raw: bytes, file_type: str) -> list[dict[str, Any]]:
        ext = file_type if file_type.startswith(".") else f".{file_type}"
        backend = self._resolve_backend(ext)

        if backend in ("pypdf", "python-docx"):
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                tmp.write(raw)
                tmp_path = Path(tmp.name)
            try:
                return self._parse_file(tmp_path, ext)
            finally:
                os.unlink(tmp_path)

        text = raw.decode("utf-8")
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
            return self._parse_pdf(file_path)
        if backend == "python-docx":
            return self._parse_docx(file_path)

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

    def _parse_text(self, file_path: Path) -> list[dict[str, Any]]:
        text = file_path.read_text(encoding="utf-8")
        return [{"text": text, "metadata": {}}]

    def _parse_csv(self, file_path: Path) -> list[dict[str, Any]]:
        text = file_path.read_text(encoding="utf-8")
        return self._parse_csv_text(text)

    def _parse_csv_text(self, text: str) -> list[dict[str, Any]]:
        reader = csv.DictReader(io.StringIO(text))
        documents = []
        for row_idx, row in enumerate(reader):
            row_text = ", ".join(f"{k}: {v}" for k, v in row.items() if v)
            documents.append(
                {
                    "text": row_text,
                    "metadata": {"row_index": row_idx},
                }
            )
        return documents

    def _parse_json(self, file_path: Path) -> list[dict[str, Any]]:
        text = file_path.read_text(encoding="utf-8")
        return self._parse_json_text(text)

    def _parse_json_text(self, text: str) -> list[dict[str, Any]]:
        data = json.loads(text)
        if isinstance(data, list):
            return [
                {"text": json.dumps(item, ensure_ascii=False), "metadata": {"item_index": idx}}
                for idx, item in enumerate(data)
            ]
        return [{"text": json.dumps(data, ensure_ascii=False), "metadata": {}}]

    def _parse_pdf(self, file_path: Path) -> list[dict[str, Any]]:
        try:
            from pypdf import PdfReader
        except ImportError:
            raise ImportError(
                "pypdf is required for PDF parsing. "
                "Install it with: pip install apache-airflow-providers-common-ai[pdf]"
            )

        reader = PdfReader(str(file_path))
        documents = []
        for page_num, page in enumerate(reader.pages):
            text = page.extract_text() or ""
            if text.strip():
                documents.append(
                    {
                        "text": text,
                        "metadata": {"page_number": page_num + 1},
                    }
                )
        return documents

    def _parse_docx(self, file_path: Path) -> list[dict[str, Any]]:
        try:
            from docx import Document
        except ImportError:
            raise ImportError(
                "python-docx is required for DOCX parsing. "
                "Install it with: pip install apache-airflow-providers-common-ai[docx]"
            )

        doc = Document(str(file_path))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        text = "\n\n".join(paragraphs)
        return [{"text": text, "metadata": {}}]
