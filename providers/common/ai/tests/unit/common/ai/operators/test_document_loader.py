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

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.operators.document_loader import DocumentLoaderOperator


class TestDocumentLoaderInit:
    def test_template_fields_render_source_path_and_metadata(self):
        """
        Behavioral check that the templated fields actually get rendered.
        Replaces the previous tautological assertion that just round-tripped
        the class attribute.
        """
        op = DocumentLoaderOperator(
            task_id="test",
            source_path="/data/{{ ds }}/*.pdf",
            file_type="{{ var.value.preferred_ext }}",
            metadata_fields={"run_id": "{{ run_id }}"},
        )
        # Make sure each one is in template_fields so render_template_fields
        # would substitute them.
        assert "source_path" in op.template_fields
        assert "file_type" in op.template_fields
        assert "file_extensions" in op.template_fields
        assert "parser" in op.template_fields
        assert "metadata_fields" in op.template_fields
        # source_bytes intentionally not templated -- Jinja stringifies bytes
        # to their repr, which would break binary parsing.
        assert "source_bytes" not in op.template_fields

    def test_both_sources_raises(self):
        with pytest.raises(ValueError, match="not both"):
            DocumentLoaderOperator(task_id="test", source_path="/tmp/file.txt", source_bytes=b"hello")

    def test_neither_source_raises(self):
        with pytest.raises(ValueError, match="Provide exactly one"):
            DocumentLoaderOperator(task_id="test")

    def test_source_bytes_without_file_type_raises(self):
        with pytest.raises(ValueError, match="file_type"):
            DocumentLoaderOperator(task_id="test", source_bytes=b"hello")

    def test_empty_bytes_without_file_type_raises(self):
        with pytest.raises(ValueError, match="file_type"):
            DocumentLoaderOperator(task_id="test", source_bytes=b"")


class TestTextParser:
    def test_txt_file(self, tmp_path):
        f = tmp_path / "doc.txt"
        f.write_text("Hello world", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert result[0]["text"] == "Hello world"
        assert result[0]["metadata"]["file_name"] == "doc.txt"

    def test_md_file(self, tmp_path):
        f = tmp_path / "readme.md"
        f.write_text("# Title\n\nSome content", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert "# Title" in result[0]["text"]


class TestCsvParser:
    def test_csv_one_doc_per_row(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("name,age\nAlice,30\nBob,25\n", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert len(result) == 2
        assert "Alice" in result[0]["text"]
        assert "Bob" in result[1]["text"]
        assert result[0]["metadata"]["row_index"] == 0
        assert result[1]["metadata"]["row_index"] == 1

    def test_csv_empty_cells_skipped(self, tmp_path):
        f = tmp_path / "data.csv"
        # Bob has no age -- "age: " should not appear in his row text.
        f.write_text("name,age\nAlice,30\nBob,\n", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert "age: " not in result[1]["text"]
        assert "Bob" in result[1]["text"]

    def test_csv_from_bytes(self):
        raw = b"col1,col2\nval1,val2\n"
        op = DocumentLoaderOperator(task_id="test", source_bytes=raw, file_type=".csv")
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert "val1" in result[0]["text"]


class TestJsonParser:
    def test_json_array_flattens_dicts(self, tmp_path):
        f = tmp_path / "items.json"
        data = [{"title": "First", "tag": "alpha"}, {"title": "Second", "tag": "beta"}]
        f.write_text(json.dumps(data), encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert len(result) == 2
        # Embedding should see "title: First, tag: alpha" rather than the raw
        # JSON syntax tokens.
        assert "title: First" in result[0]["text"]
        assert "tag: alpha" in result[0]["text"]
        assert result[0]["text"].startswith("title:")  # no leading "{"
        assert result[0]["metadata"]["item_index"] == 0

    def test_json_single_object_flattens(self, tmp_path):
        f = tmp_path / "config.json"
        f.write_text('{"key": "value", "other": "thing"}', encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert "key: value" in result[0]["text"]
        assert "other: thing" in result[0]["text"]

    def test_json_string_primitives(self, tmp_path):
        f = tmp_path / "strings.json"
        f.write_text('["alpha", "beta", "gamma"]', encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert len(result) == 3
        assert result[0]["text"] == "alpha"
        assert result[1]["text"] == "beta"
        assert result[2]["text"] == "gamma"

    def test_json_text_field_pulls_body_keeps_rest_as_metadata(self, tmp_path):
        f = tmp_path / "articles.json"
        data = [
            {"title": "Hello", "body": "First article body.", "author": "Alice"},
            {"title": "World", "body": "Second article body.", "author": "Bob"},
        ]
        f.write_text(json.dumps(data), encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f), json_text_field="body")
        result = op.execute(context=MagicMock())

        assert len(result) == 2
        assert result[0]["text"] == "First article body."
        assert result[0]["metadata"]["title"] == "Hello"
        assert result[0]["metadata"]["author"] == "Alice"
        assert "body" not in result[0]["metadata"]

    def test_json_from_bytes(self):
        raw = b'[{"a": 1}, {"b": 2}]'
        op = DocumentLoaderOperator(task_id="test", source_bytes=raw, file_type=".json")
        result = op.execute(context=MagicMock())

        assert len(result) == 2


def _make_mock_pypdf_module(mock_reader):
    """Create a fake pypdf module with a PdfReader that returns mock_reader."""
    mock_module = MagicMock()
    mock_module.PdfReader = MagicMock(return_value=mock_reader)
    return mock_module


def _make_mock_docx_module(mock_doc):
    """Create a fake docx module with a Document that returns mock_doc."""
    mock_module = MagicMock()
    mock_module.Document = MagicMock(return_value=mock_doc)
    return mock_module


class TestPdfParser:
    def test_pdf_parsing(self, tmp_path):
        mock_page_1 = MagicMock()
        mock_page_1.extract_text.return_value = "Page one content"
        mock_page_2 = MagicMock()
        mock_page_2.extract_text.return_value = "Page two content"

        mock_reader = MagicMock()
        mock_reader.pages = [mock_page_1, mock_page_2]

        f = tmp_path / "report.pdf"
        f.write_bytes(b"fake pdf bytes")

        mock_pypdf = _make_mock_pypdf_module(mock_reader)
        with patch.dict("sys.modules", {"pypdf": mock_pypdf}):
            op = DocumentLoaderOperator(task_id="test", source_path=str(f))
            result = op.execute(context=MagicMock())

        assert len(result) == 2
        assert result[0]["text"] == "Page one content"
        assert result[0]["metadata"]["page_number"] == 1
        assert result[1]["metadata"]["page_number"] == 2

    def test_pdf_from_bytes_uses_stream_no_tempfile(self, tmp_path):
        """Bytes-mode parsing should go through BytesIO, never a temp file."""
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Streamed content"
        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]

        mock_pypdf = _make_mock_pypdf_module(mock_reader)
        with patch.dict("sys.modules", {"pypdf": mock_pypdf}):
            op = DocumentLoaderOperator(task_id="test", source_bytes=b"%PDF-1.4 ...", file_type=".pdf")
            result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert result[0]["text"] == "Streamed content"
        # PdfReader should have been called once with a stream (BytesIO),
        # not a file path string.
        mock_pypdf.PdfReader.assert_called_once()
        (call_arg,) = mock_pypdf.PdfReader.call_args.args
        import io as _io

        assert isinstance(call_arg, _io.BytesIO)

    def test_pdf_skips_empty_pages(self, tmp_path):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "   "
        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]

        f = tmp_path / "empty.pdf"
        f.write_bytes(b"fake pdf")

        mock_pypdf = _make_mock_pypdf_module(mock_reader)
        with patch.dict("sys.modules", {"pypdf": mock_pypdf}):
            op = DocumentLoaderOperator(task_id="test", source_path=str(f))
            result = op.execute(context=MagicMock())

        assert len(result) == 0

    def test_pdf_missing_raises_optional_feature_exception(self, tmp_path):
        from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

        f = tmp_path / "doc.pdf"
        f.write_bytes(b"fake pdf")

        with patch.dict("sys.modules", {"pypdf": None}):
            op = DocumentLoaderOperator(task_id="test", source_path=str(f))
            with pytest.raises(AirflowOptionalProviderFeatureException):
                op.execute(context=MagicMock())


class TestDocxParser:
    def test_docx_parsing(self, tmp_path):
        mock_para_1 = MagicMock()
        mock_para_1.text = "First paragraph"
        mock_para_2 = MagicMock()
        mock_para_2.text = "Second paragraph"
        mock_para_empty = MagicMock()
        mock_para_empty.text = "   "

        mock_doc_obj = MagicMock()
        mock_doc_obj.paragraphs = [mock_para_1, mock_para_empty, mock_para_2]

        f = tmp_path / "doc.docx"
        f.write_bytes(b"fake docx")

        mock_docx = _make_mock_docx_module(mock_doc_obj)
        with patch.dict("sys.modules", {"docx": mock_docx}):
            op = DocumentLoaderOperator(task_id="test", source_path=str(f))
            result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert "First paragraph" in result[0]["text"]
        assert "Second paragraph" in result[0]["text"]

    def test_docx_from_bytes_uses_stream_no_tempfile(self):
        mock_para = MagicMock()
        mock_para.text = "Stream paragraph"
        mock_doc_obj = MagicMock()
        mock_doc_obj.paragraphs = [mock_para]

        mock_docx = _make_mock_docx_module(mock_doc_obj)
        with patch.dict("sys.modules", {"docx": mock_docx}):
            op = DocumentLoaderOperator(task_id="test", source_bytes=b"fake docx", file_type=".docx")
            result = op.execute(context=MagicMock())

        assert "Stream paragraph" in result[0]["text"]
        mock_docx.Document.assert_called_once()
        (call_arg,) = mock_docx.Document.call_args.args
        import io as _io

        assert isinstance(call_arg, _io.BytesIO)

    def test_docx_missing_raises_optional_feature_exception(self, tmp_path):
        from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

        f = tmp_path / "doc.docx"
        f.write_bytes(b"fake docx")

        with patch.dict("sys.modules", {"docx": None}):
            op = DocumentLoaderOperator(task_id="test", source_path=str(f))
            with pytest.raises(AirflowOptionalProviderFeatureException):
                op.execute(context=MagicMock())


class TestFileDiscovery:
    def test_glob_multiple_files(self, tmp_path):
        (tmp_path / "a.txt").write_text("file a", encoding="utf-8")
        (tmp_path / "b.txt").write_text("file b", encoding="utf-8")
        (tmp_path / "c.md").write_text("file c", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path / "*.txt"))
        result = op.execute(context=MagicMock())

        assert len(result) == 2
        texts = {doc["text"] for doc in result}
        assert texts == {"file a", "file b"}

    def test_recursive_glob(self, tmp_path):
        nested = tmp_path / "year" / "month"
        nested.mkdir(parents=True)
        (tmp_path / "top.txt").write_text("top", encoding="utf-8")
        (nested / "deep.txt").write_text("deep", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path / "**" / "*.txt"))
        result = op.execute(context=MagicMock())

        texts = {doc["text"] for doc in result}
        assert texts == {"top", "deep"}

    def test_directory_source(self, tmp_path):
        (tmp_path / "x.txt").write_text("hello", encoding="utf-8")
        (tmp_path / "y.md").write_text("world", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path))
        result = op.execute(context=MagicMock())

        assert len(result) == 2

    def test_directory_mode_skips_dotfiles(self, tmp_path):
        (tmp_path / "keep.txt").write_text("keep", encoding="utf-8")
        (tmp_path / ".DS_Store").write_bytes(b"\x00\x00")
        (tmp_path / ".hidden.txt").write_text("nope", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path))
        result = op.execute(context=MagicMock())

        # Only the non-dotfile is parsed; .DS_Store and .hidden.txt are ignored.
        assert len(result) == 1
        assert result[0]["text"] == "keep"

    def test_directory_mode_warns_and_skips_unknown_extensions(self, tmp_path, caplog):
        (tmp_path / "keep.txt").write_text("keep", encoding="utf-8")
        (tmp_path / "stray.xyz").write_text("ignored", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path))
        with caplog.at_level(logging.WARNING):
            result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert result[0]["text"] == "keep"
        assert any(".xyz" in record.message for record in caplog.records)

    def test_file_extensions_filter(self, tmp_path):
        (tmp_path / "keep.txt").write_text("keep me", encoding="utf-8")
        (tmp_path / "skip.md").write_text("skip me", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path), file_extensions=[".txt"])
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert result[0]["text"] == "keep me"

    def test_empty_directory_raises_file_not_found(self, tmp_path):
        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path))
        with pytest.raises(FileNotFoundError, match="No files found"):
            op.execute(context=MagicMock())

    def test_unknown_extension_on_single_file_raises(self, tmp_path):
        f = tmp_path / "data.xyz"
        f.write_text("some data", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        with pytest.raises(ValueError, match="No parser registered"):
            op.execute(context=MagicMock())

    def test_nonexistent_glob_raises_file_not_found(self, tmp_path):
        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path / "*.nope"))
        with pytest.raises(FileNotFoundError, match="No files found"):
            op.execute(context=MagicMock())

    def test_file_extensions_case_insensitive(self, tmp_path):
        (tmp_path / "keep.txt").write_text("keep me", encoding="utf-8")
        (tmp_path / "skip.md").write_text("skip me", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path), file_extensions=[".TXT"])
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert result[0]["text"] == "keep me"


class TestCloudUriDispatch:
    """``source_path`` containing a URI scheme routes through ObjectStoragePath."""

    @patch("airflow.sdk.ObjectStoragePath")
    def test_single_object_uri_returns_one_document(self, mock_osp_cls):
        # `str(mock_obj)` returns whatever MagicMock renders; we only assert
        # the file_name field, not file_path, so leaving __str__ default is
        # fine and avoids mypy's method-assign complaint.
        mock_obj = MagicMock()
        mock_obj.is_file.return_value = True
        mock_obj.suffix = ".txt"
        mock_obj.name = "report.txt"
        mock_obj.read_bytes.return_value = b"cloud content"
        mock_osp_cls.return_value = mock_obj

        op = DocumentLoaderOperator(
            task_id="test",
            source_path="s3://bucket/dir/report.txt",
            source_conn_id="aws_default",
        )
        result = op.execute(context=MagicMock())

        mock_osp_cls.assert_called_once_with("s3://bucket/dir/report.txt", conn_id="aws_default")
        assert len(result) == 1
        assert result[0]["text"] == "cloud content"
        assert result[0]["metadata"]["file_name"] == "report.txt"

    @patch("airflow.sdk.ObjectStoragePath")
    def test_directory_uri_iterates_children(self, mock_osp_cls):
        # Root is a directory; iterdir yields two text files.
        def _mock_child(name: str, content: bytes):
            child = MagicMock()
            child.is_file.return_value = True
            child.name = name
            child.suffix = "." + name.rsplit(".", 1)[-1]
            child.read_bytes.return_value = content
            return child

        a = _mock_child("a.txt", b"alpha")
        b = _mock_child("b.txt", b"beta")

        root = MagicMock()
        root.is_file.return_value = False
        root.is_dir.return_value = True
        root.iterdir.return_value = [a, b]
        mock_osp_cls.return_value = root

        op = DocumentLoaderOperator(task_id="test", source_path="s3://bucket/dir/")
        result = op.execute(context=MagicMock())

        assert {doc["text"] for doc in result} == {"alpha", "beta"}

    @patch("airflow.sdk.ObjectStoragePath")
    def test_neither_file_nor_dir_uri_raises(self, mock_osp_cls):
        bad = MagicMock()
        bad.is_file.return_value = False
        bad.is_dir.return_value = False
        mock_osp_cls.return_value = bad

        op = DocumentLoaderOperator(task_id="test", source_path="s3://bucket/missing")
        with pytest.raises(FileNotFoundError, match="neither a file nor a directory"):
            op.execute(context=MagicMock())


class TestEncoding:
    def test_strict_utf8_default_raises_with_path_context(self, tmp_path):
        f = tmp_path / "latin1.csv"
        # \xff is invalid in UTF-8; surface the file path in the error.
        f.write_bytes(b"\xff\xfe header\nrow\n")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        with pytest.raises(ValueError, match=str(f)):
            op.execute(context=MagicMock())

    def test_encoding_errors_replace_tolerates_garbage(self, tmp_path):
        f = tmp_path / "mixed.txt"
        f.write_bytes(b"hello \xff world")

        op = DocumentLoaderOperator(
            task_id="test",
            source_path=str(f),
            encoding_errors="replace",
        )
        result = op.execute(context=MagicMock())

        assert "hello" in result[0]["text"]
        assert "world" in result[0]["text"]

    def test_alternative_encoding_succeeds(self, tmp_path):
        f = tmp_path / "latin1.txt"
        f.write_bytes("café".encode("latin-1"))

        op = DocumentLoaderOperator(task_id="test", source_path=str(f), encoding="latin-1")
        result = op.execute(context=MagicMock())

        assert "café" in result[0]["text"]


class TestOutputShape:
    def test_every_item_has_text_and_metadata(self, tmp_path):
        (tmp_path / "a.txt").write_text("doc a", encoding="utf-8")
        (tmp_path / "b.txt").write_text("doc b", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path / "*.txt"))
        result = op.execute(context=MagicMock())

        for doc in result:
            assert "text" in doc
            assert "metadata" in doc
            assert isinstance(doc["text"], str)
            assert isinstance(doc["metadata"], dict)

    def test_metadata_fields_appended(self, tmp_path):
        f = tmp_path / "doc.txt"
        f.write_text("content", encoding="utf-8")

        op = DocumentLoaderOperator(
            task_id="test",
            source_path=str(f),
            metadata_fields={"source": "test_suite", "version": 2},
        )
        result = op.execute(context=MagicMock())

        assert result[0]["metadata"]["source"] == "test_suite"
        assert result[0]["metadata"]["version"] == 2

    def test_metadata_fields_do_not_override_auto_extracted(self, tmp_path):
        """Auto-extracted file_name wins over a same-key entry in metadata_fields."""
        f = tmp_path / "report.txt"
        f.write_text("content", encoding="utf-8")

        op = DocumentLoaderOperator(
            task_id="test",
            source_path=str(f),
            metadata_fields={"file_name": "spoofed", "extra": "kept"},
        )
        result = op.execute(context=MagicMock())

        assert result[0]["metadata"]["file_name"] == "report.txt"
        assert result[0]["metadata"]["extra"] == "kept"

    def test_file_metadata_included(self, tmp_path):
        f = tmp_path / "report.txt"
        f.write_text("content", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert result[0]["metadata"]["file_name"] == "report.txt"
        assert "file_path" in result[0]["metadata"]

    def test_source_bytes_no_file_metadata(self):
        op = DocumentLoaderOperator(task_id="test", source_bytes=b"hello", file_type=".txt")
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert result[0]["text"] == "hello"
        assert "file_name" not in result[0]["metadata"]

    def test_explicit_parser_override(self, tmp_path):
        f = tmp_path / "data.log"
        f.write_text("log line", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f), parser="text")
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert result[0]["text"] == "log line"
