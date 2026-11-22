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

import copy
import csv
import glob
import io
import json
import posixpath
import zipfile
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO
from xml.etree import ElementTree

from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseOperator,
)

if TYPE_CHECKING:
    from docx.oxml.exceptions import InvalidXmlError
    from docx.table import Table, _Cell
    from docx.text.paragraph import Paragraph

    from airflow.sdk import Context


# Type alias for path-like inputs that the parsers can read from. ``Path`` is
# the local filesystem; ``ObjectStoragePath`` covers ``s3://``, ``gs://``,
# ``azure://``, ``file://``, ... via fsspec. Both expose the methods we need
# (``read_bytes``, ``open``, ``name``, ``suffix``) so the parsers stay
# polymorphic.
FilePathT = Any  # Path | ObjectStoragePath

# Word allows at most 63 cells in a row, but rows whose borders do not line up add grid
# columns (a 40-cell row above a 41-cell row saves with 80), so the limit sits well above
# any real table. It bounds the width a file sets through w:tblGrid or w:gridSpan, and
# wider tables are skipped.
_DOCX_TABLE_MAX_COLUMNS = 1000

# A vertically merged cell repeats its text on every row it spans, and merges nested inside
# merged cells multiply that with each level, so a small file can expand to millions of
# characters. The count includes about 3 characters of separators per cell, so padded empty
# cells count too. 1M characters is about 6000 rows of 10 columns; longer tables are skipped.
_DOCX_TABLE_MAX_CHARS = 1_000_000

# python-docx reads every part of a DOCX whole, and the XML parts it parses take 13-18x their
# size in memory (about 50x for a file of empty paragraphs). Word-saved files hold about 30 bytes
# of XML per character, so 50 MB is about 1.5M characters. Both limits are on uncompressed sizes,
# which can be far larger or smaller than the .docx file itself.
_DOCX_MAX_XML_BYTES = 50 * 1024 * 1024

# Word does not open files over 512 MB.
_DOCX_MAX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024

_CONTENT_TYPES_NS = "{http://schemas.openxmlformats.org/package/2006/content-types}"


def _build_docx_size_error(source_hint: str, parts: str, size: int, limit: int) -> ValueError:
    return ValueError(
        f"Cannot load DOCX {source_hint}: {parts} total {size / 2**20:.1f} MB uncompressed, "
        f"over the {limit / 2**20:.0f} MB limit."
    )


def _iter_docx_part_chunks(
    zf: zipfile.ZipFile, info: zipfile.ZipInfo, *, source_hint: str
) -> Iterator[bytes]:
    # ZipFile stops reading at the declared size, so read through a copy that allows one more
    # byte: a part holding more data then fails the CRC check or yields the extra byte.
    probe = copy.copy(info)
    probe.file_size += 1
    read_bytes = 0
    with zf.open(probe) as part:
        while chunk := part.read(64 * 1024):
            read_bytes += len(chunk)
            if read_bytes > info.file_size:
                raise ValueError(
                    f"Cannot load DOCX {source_hint}: part {info.filename!r} holds more data than "
                    "its declared size."
                )
            yield chunk


def _check_docx_part_sizes(stream: BinaryIO, *, source_hint: str) -> None:
    """
    Reject a DOCX whose parts are too large to load, before python-docx reads it.

    The sizes declared in the zip directory are checked first. Every part is then inflated in
    small chunks to confirm it holds no more than its declared size: ``ZipFile.read`` inflates up
    to 1 GiB (2 GiB on Python 3.14) before it cuts the data at the declared size, so a forged
    size alone would not bound memory.

    A part counts as XML when its content type ends in "xml" or it is a ``.rels`` file, which
    python-docx parses whatever its content type. Content types are looked up the way
    python-docx does it, so a part cannot escape the XML limit through its name.
    """
    with zipfile.ZipFile(stream) as zf:
        infos = zf.infolist()
        total_bytes = sum(info.file_size for info in infos)
        if total_bytes > _DOCX_MAX_UNCOMPRESSED_BYTES:
            raise _build_docx_size_error(source_hint, "its parts", total_bytes, _DOCX_MAX_UNCOMPRESSED_BYTES)

        content_types_info = zf.getinfo("[Content_Types].xml")
        if content_types_info.file_size > _DOCX_MAX_XML_BYTES:
            raise _build_docx_size_error(
                source_hint, "its XML parts", content_types_info.file_size, _DOCX_MAX_XML_BYTES
            )
        types = ElementTree.fromstring(
            b"".join(_iter_docx_part_chunks(zf, content_types_info, source_hint=source_hint))
        )
        defaults = {
            e.get("Extension", "").lower(): e.get("ContentType", "")
            for e in types.findall(f"{_CONTENT_TYPES_NS}Default")
        }
        overrides = {
            e.get("PartName", "").lower(): e.get("ContentType", "")
            for e in types.findall(f"{_CONTENT_TYPES_NS}Override")
        }

        xml_bytes = 0
        for info in infos:
            name = info.filename.lower()
            content_type = overrides.get(f"/{name}") or defaults.get(posixpath.splitext(name)[1][1:], "")
            if info is content_types_info or name.endswith(".rels") or content_type.endswith("xml"):
                xml_bytes += info.file_size
        if xml_bytes > _DOCX_MAX_XML_BYTES:
            raise _build_docx_size_error(source_hint, "its XML parts", xml_bytes, _DOCX_MAX_XML_BYTES)

        for info in infos:
            if info is not content_types_info:
                for _chunk in _iter_docx_part_chunks(zf, info, source_hint=source_hint):
                    pass
    stream.seek(0)


class DocumentLoaderOperator(BaseOperator):
    """
    Parse files into ``list[dict(text, metadata)]`` for downstream embedding.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Bridges Airflow's connectivity layer (hooks that produce bytes or local
    files) and the AI embedding layer (operators that need structured text
    with metadata). Framework-agnostic: no LlamaIndex, LangChain, or other
    AI framework dependency.

    Built-in parsers handle ``.txt``, ``.md``, ``.csv``, ``.json``, and
    ``.jsonl`` with zero extra dependencies. PDF and DOCX support require
    optional packages installable via extras::

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
        ``**`` enables recursive matching, for both local paths and cloud
        URIs. Wildcards must not appear in the scheme or bucket segment
        of a cloud URI.
    :param source_conn_id: Airflow connection ID used by
        ``ObjectStoragePath`` for cloud URIs (``aws_default``,
        ``google_cloud_default``, ...). Ignored for local paths.
    :param source_bytes: Raw file bytes, typically from XCom.
    :param file_type: File extension hint (e.g. ``".pdf"``). Required when
        using ``source_bytes``, since bytes carry no extension to detect.
        Omitting it is rejected when the operator is constructed -- Dag parse
        time for a regular task, run time for a mapped one, since ``expand()``
        validates argument names only and defers construction to ``unmap()``.
        Optional with ``source_path``, where it overrides auto-detection.
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
    :param encoding: Text encoding used for
        ``.txt``/``.md``/``.csv``/``.json``/``.jsonl`` and for the bytes path.
        Defaults to ``"utf-8"``.
    :param encoding_errors: How decode errors are handled. Defaults to
        ``"strict"``; set to ``"replace"`` or ``"ignore"`` to tolerate
        mixed-encoding inputs at the cost of some character loss.
    :param json_text_field: When parsing JSON or JSON Lines, treat this key
        as the embedding text and put every other key into ``metadata``.
        Applies to each item when the top-level JSON is a list, to the object
        when it is a single dict, or to each JSON Lines record. When ``None``
        (default), the operator flattens dicts into ``"k: v, k: v"`` text
        (same shape as the CSV parser).
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
        ".jsonl": "jsonl",
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
        # Provision -- whether an argument was supplied at all -- is settled in __init__.
        # Both guards below exist for a different reason: file_type and source_path are
        # template fields, so a supplied argument can still arrive here as None once it has
        # been rendered. __init__ cannot catch that, because it only ever sees the unrendered
        # template string; _parse_bytes and _resolve_files need the rendered value to be real.
        if self.source_bytes is not None and self.file_type is None:
            raise ValueError(
                "'file_type' was supplied but rendered to None. Check the template or the "
                "upstream XCom value it resolves from."
            )
        if self.source_bytes is None and self.source_path is None:
            raise ValueError(
                "'source_path' was supplied but rendered to None. Check the template or the "
                "upstream XCom value it resolves from."
            )

        if self.source_bytes is not None:
            if TYPE_CHECKING:
                assert self.file_type is not None
            documents = self._parse_bytes(self.source_bytes, self.file_type)
            file_count = 1
        else:
            if TYPE_CHECKING:
                assert self.source_path is not None
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
        from airflow.providers.common.compat.sdk import ObjectStoragePath

        if any(char in source_path for char in "*?["):
            return self._resolve_remote_glob(source_path)

        root = ObjectStoragePath(source_path, conn_id=self.source_conn_id)
        try:
            if root.is_file():
                return [root]
        except FileNotFoundError:
            # Some fsspec backends raise instead of returning False.
            pass

        if not root.is_dir():
            raise FileNotFoundError(f"Cloud URI '{source_path}' is neither a file nor a directory.")

        candidates = sorted(
            (p for p in root.iterdir() if not p.name.startswith(".")),
            key=str,
        )
        return self._filter_files([p for p in candidates if p.is_file()], is_directory_mode=True)

    def _resolve_remote_glob(self, source_path: str) -> list[FilePathT]:
        from airflow.providers.common.compat.sdk import ObjectStoragePath

        segments = source_path.split("/")
        magic_at = next(i for i, seg in enumerate(segments) if any(c in seg for c in "*?["))
        # segments[:3] is ``scheme:``, ``""``, ``bucket``; a wildcard there has no fixed root.
        if magic_at < 3:
            raise ValueError(
                f"Cloud URI '{source_path}' must not use wildcards in the scheme or bucket segment."
            )

        root = ObjectStoragePath("/".join(segments[:magic_at]), conn_id=self.source_conn_id)
        candidates = sorted(root.glob("/".join(segments[magic_at:])), key=str)
        return self._filter_files([p for p in candidates if p.is_file()], is_directory_mode=False)

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
        source_hint = f"<bytes:{ext}>"

        if backend == "pypdf":
            return self._parse_pdf_stream(io.BytesIO(raw))
        if backend == "python-docx":
            return self._parse_docx_stream(io.BytesIO(raw), source_hint=source_hint)

        text = self._decode(raw, source_hint=source_hint)
        if backend == "csv":
            return self._parse_csv_text(text)
        if backend == "json":
            return self._parse_json_text(text)
        if backend == "jsonl":
            return self._parse_json_lines_text(text, source_hint=source_hint)
        return [{"text": text, "metadata": {}}]

    def _parse_file(self, file_path: Path, ext: str) -> list[dict[str, Any]]:
        backend = self._resolve_backend(ext)

        if backend == "text":
            return self._parse_text(file_path)
        if backend == "csv":
            return self._parse_csv(file_path)
        if backend == "json":
            return self._parse_json(file_path)
        if backend == "jsonl":
            return self._parse_json_lines(file_path)
        if backend == "pypdf":
            with file_path.open("rb") as fh:
                return self._parse_pdf_stream(fh)
        if backend == "python-docx":
            with file_path.open("rb") as fh:
                return self._parse_docx_stream(fh, source_hint=str(file_path))

        raise ValueError(f"No parser found for backend '{backend}'.")

    def _resolve_backend(self, ext: str) -> str:
        if self.parser != "auto":
            if self.parser not in set(self.EXTENSION_BACKEND_MAP.values()):
                raise ValueError(f"No parser found for backend '{self.parser}'.")
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

    def _parse_json_lines(self, file_path: Path) -> list[dict[str, Any]]:
        return self._parse_json_lines_text(self._read_text(file_path), source_hint=str(file_path))

    def _parse_json_lines_text(self, text: str, *, source_hint: str) -> list[dict[str, Any]]:
        documents: list[dict[str, Any]] = []
        # split("\n") rather than splitlines(): JSON Lines is defined with \n, while splitlines()
        # also breaks on U+2028/U+2029/U+0085, which are legal unescaped characters inside a JSON
        # string and would tear a valid record in half. A trailing \r from CRLF is JSON whitespace.
        for line_number, line in enumerate(text.split("\n"), start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Failed to parse {source_hint}: "
                    f"invalid JSON on line {line_number}, column {e.colno}: {e.msg}"
                ) from e
            documents.append(self._json_item_to_doc(item, item_index=len(documents)))
        return documents

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

    def _parse_docx_stream(self, stream: BinaryIO, *, source_hint: str) -> list[dict[str, Any]]:
        """
        Parse a DOCX stream into a single document.

        Paragraphs and tables in the document body are extracted in document
        order. Each table row becomes one "| cell | cell |" line, and a nested
        table is flattened into its cell. Headers, footers, footnotes, content
        controls, text boxes, and pending tracked insertions are not included.
        """
        try:
            from docx import Document
            from docx.oxml.exceptions import InvalidXmlError
            from docx.table import Table, _Cell
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)

        _check_docx_part_sizes(stream, source_hint=source_hint)
        doc = Document(stream)
        blocks = []
        for block in doc.iter_inner_content():
            if isinstance(block, Table):
                rows = self._get_docx_table_rows_or_none(
                    block, Table, _Cell, InvalidXmlError, source_hint=source_hint
                )
                if rows is None:
                    continue
                text = "\n".join(f"| {' | '.join(cells)} |" for cells in rows)
            else:
                text = block.text
            if text.strip():
                blocks.append(text)
        text = "\n\n".join(blocks)
        return [{"text": text, "metadata": {}}]

    def _get_docx_table_rows_or_none(
        self,
        table: Table,
        table_cls: type[Table],
        cell_cls: type[_Cell],
        invalid_xml_error_cls: type[InvalidXmlError],
        *,
        source_hint: str,
    ) -> list[list[str]] | None:
        try:
            return self._get_docx_table_rows(
                table, table_cls, cell_cls, invalid_xml_error_cls, source_hint=source_hint
            )
        except (ValueError, invalid_xml_error_cls) as e:
            # ValueError covers a w:vMerge continuation that does not line up with a cell above,
            # a w:gridSpan that does not fit the table, a table wider than _DOCX_TABLE_MAX_COLUMNS,
            # and a table whose text exceeds _DOCX_TABLE_MAX_CHARS; InvalidXmlError covers a
            # missing required attribute. Nested tables are read recursively, but python-docx
            # parses without huge_tree, so libxml2 refuses a file nested more than 256 elements
            # deep (83 tables) and the recursion stays far below Python's limit.
            self.log.warning("Skipping a table in %s: %r", source_hint, e)
            return None

    def _get_docx_table_rows(
        self,
        table: Table,
        table_cls: type[Table],
        cell_cls: type[_Cell],
        invalid_xml_error_cls: type[InvalidXmlError],
        *,
        source_hint: str,
    ) -> list[list[str]]:
        # w:gridBefore and w:gridAfter come straight from the XML and can be any integer, so the
        # empty cells added for them only fill the row up to the column count.
        try:
            column_count = len(table.columns)
        except invalid_xml_error_cls:
            # Apache POI writes tables without w:tblGrid, so the widest row sets the column count.
            column_count = max(
                (
                    max(row.grid_cols_before, 0)
                    + sum(tc.grid_span for tc in row._tr.tc_lst)
                    + max(row.grid_cols_after, 0)
                    for row in table.rows
                ),
                default=0,
            )
        if column_count > _DOCX_TABLE_MAX_COLUMNS:
            raise ValueError(
                f"the table has {column_count} columns, which exceeds the maximum of {_DOCX_TABLE_MAX_COLUMNS}"
            )

        rows = []
        char_count = 0
        # row.cells is not used: it finds the text of each w:vMerge continuation by recursing up
        # to the merge start, which is quadratic in the merge height and overflows the stack at
        # about 1000 rows. Instead the text and w:gridSpan of each cell is carried down from the
        # row above, keyed by the grid column the cell starts at.
        above: dict[int, tuple[str, int]] = {}
        for row in table.rows:
            tcs = row._tr.tc_lst
            free_columns = column_count - sum(tc.grid_span for tc in tcs)
            grid_before = max(0, min(row.grid_cols_before, free_columns))
            grid_after = max(0, min(row.grid_cols_after, free_columns - grid_before))

            current: dict[int, tuple[str, int]] = {}
            cells = [""] * grid_before
            offset = grid_before
            for index, tc in enumerate(tcs, start=1):
                span = tc.grid_span
                # Each cell fills one column for every grid column its w:gridSpan covers, so a
                # span below 1, or one that runs past the table width, is rejected before any
                # column is built.
                if span < 1 or offset + span > column_count:
                    raise ValueError(
                        f"w:gridSpan {span} of cell {index} does not fit the {column_count} table columns"
                    )
                if tc.vMerge == "continue":
                    if offset not in above or above[offset][1] != span:
                        raise ValueError(
                            f"w:vMerge continuation of cell {index} does not line up with a cell above it"
                        )
                    text = above[offset][0]
                else:
                    text = self._get_docx_cell_text(
                        cell_cls(tc, table),
                        table_cls,
                        cell_cls,
                        invalid_xml_error_cls,
                        source_hint=source_hint,
                    )
                current[offset] = (text, span)
                # A horizontal merge keeps its text once and leaves its other columns empty so
                # later cells stay under their headers. A vertical merge repeats on each row.
                cells += [text] + [""] * (span - 1)
                offset += span
            cells += [""] * grid_after
            # Merges starting in a row that is left out for being empty still continue below it.
            above = current
            if any(cells):
                char_count += sum(map(len, cells)) + 3 * len(cells)
                if char_count > _DOCX_TABLE_MAX_CHARS:
                    raise ValueError(
                        f"the table text exceeds the maximum of {_DOCX_TABLE_MAX_CHARS} characters"
                    )
                rows.append(cells)
        return rows

    def _get_docx_cell_text(
        self,
        cell: _Cell,
        table_cls: type[Table],
        cell_cls: type[_Cell],
        invalid_xml_error_cls: type[InvalidXmlError],
        *,
        source_hint: str,
    ) -> str:
        parts = []
        for item in cell.iter_inner_content():
            if isinstance(item, table_cls):
                rows = self._get_docx_table_rows_or_none(
                    item, table_cls, cell_cls, invalid_xml_error_cls, source_hint=source_hint
                )
                if rows is None:
                    continue
                text = "; ".join(" / ".join(cells) for cells in rows)
            else:
                if TYPE_CHECKING:
                    assert isinstance(item, Paragraph)
                text = " ".join(item.text.split())
            if text:
                parts.append(text)
        return " ".join(parts)
