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
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.operators.document_loader import DocumentLoaderOperator


class TestDocumentLoaderInit:
    def test_template_fields(self):
        expected = {"source_path", "source_bytes", "metadata_fields"}
        assert set(DocumentLoaderOperator.template_fields) == expected

    def test_both_sources_raises(self):
        with pytest.raises(ValueError, match="not both"):
            DocumentLoaderOperator(task_id="test", source_path="/tmp/file.txt", source_bytes=b"hello")

    def test_neither_source_raises(self):
        with pytest.raises(ValueError, match="Provide exactly one"):
            DocumentLoaderOperator(task_id="test")

    def test_source_bytes_without_file_type_raises(self):
        with pytest.raises(ValueError, match="file_type"):
            DocumentLoaderOperator(task_id="test", source_bytes=b"hello")


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

    def test_csv_from_bytes(self):
        raw = b"col1,col2\nval1,val2\n"
        op = DocumentLoaderOperator(task_id="test", source_bytes=raw, file_type=".csv")
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert "val1" in result[0]["text"]


class TestJsonParser:
    def test_json_array(self, tmp_path):
        f = tmp_path / "items.json"
        data = [{"title": "First"}, {"title": "Second"}]
        f.write_text(json.dumps(data), encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert len(result) == 2
        assert result[0]["metadata"]["item_index"] == 0

    def test_json_single_object(self, tmp_path):
        f = tmp_path / "config.json"
        f.write_text('{"key": "value"}', encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert "key" in result[0]["text"]

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

    def test_pdf_missing_raises_importerror(self, tmp_path):
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"fake pdf")

        with patch.dict("sys.modules", {"pypdf": None}):
            op = DocumentLoaderOperator(task_id="test", source_path=str(f))
            with pytest.raises(ImportError, match="apache-airflow-providers-common-ai\\[pdf\\]"):
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

    def test_docx_missing_raises_importerror(self, tmp_path):
        f = tmp_path / "doc.docx"
        f.write_bytes(b"fake docx")

        with patch.dict("sys.modules", {"docx": None}):
            op = DocumentLoaderOperator(task_id="test", source_path=str(f))
            with pytest.raises(ImportError, match="apache-airflow-providers-common-ai\\[docx\\]"):
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

    def test_directory_source(self, tmp_path):
        (tmp_path / "x.txt").write_text("hello", encoding="utf-8")
        (tmp_path / "y.md").write_text("world", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path))
        result = op.execute(context=MagicMock())

        assert len(result) == 2

    def test_file_extensions_filter(self, tmp_path):
        (tmp_path / "keep.txt").write_text("keep me", encoding="utf-8")
        (tmp_path / "skip.md").write_text("skip me", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path), file_extensions=[".txt"])
        result = op.execute(context=MagicMock())

        assert len(result) == 1
        assert result[0]["text"] == "keep me"

    def test_empty_directory_returns_empty(self, tmp_path):
        op = DocumentLoaderOperator(task_id="test", source_path=str(tmp_path))
        result = op.execute(context=MagicMock())

        assert result == []

    def test_unknown_extension_raises(self, tmp_path):
        f = tmp_path / "data.xyz"
        f.write_text("some data", encoding="utf-8")

        op = DocumentLoaderOperator(task_id="test", source_path=str(f))
        with pytest.raises(ValueError, match="No parser registered"):
            op.execute(context=MagicMock())


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
