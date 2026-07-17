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

import builtins
import gzip
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai.messages import BinaryContent

from airflow.providers.common.ai.exceptions import (
    LLMFileAnalysisLimitExceededError,
    LLMFileAnalysisMultimodalRequiredError,
    LLMFileAnalysisUnsupportedFormatError,
)
from airflow.providers.common.ai.utils.file_analysis import (
    FileAnalysisRequest,
    _infer_partitions,
    _read_raw_bytes,
    _render_avro,
    _render_parquet,
    _resolve_paths,
    _truncate_text,
    build_file_analysis_request,
    detect_file_format,
)
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, ObjectStoragePath

AIRFLOW_PNG_PATH = Path(__file__).resolve().parents[1] / "assets" / "airflow-3-task-sdk.png"


class TestBuildFileAnalysisRequest:
    def test_text_file_analysis(self, tmp_path):
        path = tmp_path / "app.log"
        path.write_text("first line\nsecond line\n", encoding="utf-8")

        request = build_file_analysis_request(
            file_path=str(path),
            file_conn_id=None,
            prompt="Summarize the log",
            multi_modal=False,
            max_files=20,
            max_file_size_bytes=1024,
            max_total_size_bytes=2048,
            max_text_chars=500,
            sample_rows=10,
        )

        assert isinstance(request, FileAnalysisRequest)
        assert request.resolved_paths == [str(path)]
        assert "Summarize the log" in request.user_content
        assert "first line" in request.user_content
        assert "format=log" in request.user_content

    def test_file_conn_id_overrides_embedded_connection(self, tmp_path):
        path = tmp_path / "data.json"
        path.write_text('{"status": "ok"}', encoding="utf-8")

        with patch(
            "airflow.providers.common.ai.utils.file_analysis.ObjectStoragePath", autospec=True
        ) as mock_path:
            mock_instance = MagicMock(spec=ObjectStoragePath)
            mock_handle = MagicMock(spec=["read"])
            mock_handle.read.side_effect = [b'{"status": "ok"}', b""]
            mock_instance.is_file.return_value = True
            mock_instance.stat.return_value.st_size = 16
            mock_instance.suffixes = [".json"]
            mock_instance.path = str(path)
            mock_instance.open.return_value.__enter__.return_value = mock_handle
            mock_path.return_value = mock_instance

            build_file_analysis_request(
                file_path="s3://embedded_conn@bucket/data.json",
                file_conn_id="override_conn",
                prompt="Inspect",
                multi_modal=False,
                max_files=1,
                max_file_size_bytes=1024,
                max_total_size_bytes=1024,
                max_text_chars=500,
                sample_rows=10,
            )

        mock_path.assert_called_once_with("s3://embedded_conn@bucket/data.json", conn_id="override_conn")

    def test_sample_rows_must_be_positive(self, tmp_path):
        path = tmp_path / "metrics.csv"
        path.write_text("name,value\nalpha,1\n", encoding="utf-8")

        with pytest.raises(ValueError, match="sample_rows must be greater than zero"):
            build_file_analysis_request(
                file_path=str(path),
                file_conn_id=None,
                prompt="Analyze",
                multi_modal=False,
                max_files=1,
                max_file_size_bytes=1024,
                max_total_size_bytes=1024,
                max_text_chars=500,
                sample_rows=0,
            )

    def test_prefix_aggregation_is_sorted_and_omits_extra_files(self, tmp_path):
        (tmp_path / "b.log").write_text("b", encoding="utf-8")
        (tmp_path / "a.log").write_text("a", encoding="utf-8")
        (tmp_path / "c.log").write_text("c", encoding="utf-8")

        request = build_file_analysis_request(
            file_path=str(tmp_path),
            file_conn_id=None,
            prompt="Summarize",
            multi_modal=False,
            max_files=2,
            max_file_size_bytes=1024,
            max_total_size_bytes=2048,
            max_text_chars=500,
            sample_rows=10,
        )

        assert request.resolved_paths == [str(tmp_path / "a.log"), str(tmp_path / "b.log")]
        assert request.omitted_files == 1
        assert "omitted_files=1" in request.user_content

    def test_multi_modal_image_creates_binary_attachment(self):
        request = build_file_analysis_request(
            file_path=str(AIRFLOW_PNG_PATH),
            file_conn_id=None,
            prompt="Check for anomalies",
            multi_modal=True,
            max_files=1,
            max_file_size_bytes=256 * 1024,
            max_total_size_bytes=256 * 1024,
            max_text_chars=500,
            sample_rows=10,
        )

        assert isinstance(request.user_content, list)
        assert isinstance(request.user_content[1], BinaryContent)
        assert request.user_content[1].media_type == "image/png"
        assert request.resolved_paths == [str(AIRFLOW_PNG_PATH)]
        assert "attached_as_binary=True" in request.user_content[0]

    def test_multimodal_required_for_image_and_pdf(self, tmp_path):
        path = tmp_path / "report.pdf"
        path.write_bytes(b"%PDF-1.7")

        with pytest.raises(LLMFileAnalysisMultimodalRequiredError, match="multi_modal=True"):
            build_file_analysis_request(
                file_path=str(path),
                file_conn_id=None,
                prompt="Analyze",
                multi_modal=False,
                max_files=1,
                max_file_size_bytes=1024,
                max_total_size_bytes=1024,
                max_text_chars=500,
                sample_rows=10,
            )

    def test_unsupported_suffix_raises(self, tmp_path):
        path = tmp_path / "payload.bin"
        path.write_bytes(b"abc")

        with pytest.raises(LLMFileAnalysisUnsupportedFormatError, match="Unsupported file format"):
            build_file_analysis_request(
                file_path=str(path),
                file_conn_id=None,
                prompt="Analyze",
                multi_modal=False,
                max_files=1,
                max_file_size_bytes=1024,
                max_total_size_bytes=1024,
                max_text_chars=500,
                sample_rows=10,
            )

    def test_single_file_limit_failure(self, tmp_path):
        path = tmp_path / "big.log"
        path.write_text("abcdef", encoding="utf-8")

        with patch(
            "airflow.providers.common.ai.utils.file_analysis._prepare_file", autospec=True
        ) as mock_prepare:
            with pytest.raises(LLMFileAnalysisLimitExceededError, match="per-file limit"):
                build_file_analysis_request(
                    file_path=str(path),
                    file_conn_id=None,
                    prompt="Analyze",
                    multi_modal=False,
                    max_files=1,
                    max_file_size_bytes=5,
                    max_total_size_bytes=1024,
                    max_text_chars=500,
                    sample_rows=10,
                )

        mock_prepare.assert_not_called()

    def test_total_size_limit_failure(self, tmp_path):
        (tmp_path / "a.log").write_text("abc", encoding="utf-8")
        (tmp_path / "b.log").write_text("def", encoding="utf-8")

        with patch(
            "airflow.providers.common.ai.utils.file_analysis._prepare_file", autospec=True
        ) as mock_prepare:
            with pytest.raises(LLMFileAnalysisLimitExceededError, match="Total input size exceeds"):
                build_file_analysis_request(
                    file_path=str(tmp_path),
                    file_conn_id=None,
                    prompt="Analyze",
                    multi_modal=False,
                    max_files=10,
                    max_file_size_bytes=1024,
                    max_total_size_bytes=5,
                    max_text_chars=500,
                    sample_rows=10,
                )

        mock_prepare.assert_not_called()

    def test_gzip_expansion_respects_processed_content_limit(self, tmp_path):
        path = tmp_path / "big.log.gz"
        path.write_bytes(gzip.compress(b"A" * 20_000))

        with pytest.raises(LLMFileAnalysisLimitExceededError, match="processed-content limit"):
            build_file_analysis_request(
                file_path=str(path),
                file_conn_id=None,
                prompt="Analyze",
                multi_modal=False,
                max_files=1,
                max_file_size_bytes=256,
                max_total_size_bytes=256,
                max_text_chars=500,
                sample_rows=10,
            )

    def test_gzip_expansion_respects_total_processed_content_limit(self, tmp_path):
        (tmp_path / "a.log.gz").write_bytes(gzip.compress(b"A" * 1_000))
        (tmp_path / "b.log.gz").write_bytes(gzip.compress(b"B" * 1_000))

        with pytest.raises(LLMFileAnalysisLimitExceededError, match="processed-content limit"):
            build_file_analysis_request(
                file_path=str(tmp_path),
                file_conn_id=None,
                prompt="Analyze",
                multi_modal=False,
                max_files=10,
                max_file_size_bytes=2_048,
                max_total_size_bytes=1_500,
                max_text_chars=500,
                sample_rows=10,
            )

    def test_json_is_pretty_printed(self, tmp_path):
        path = tmp_path / "data.json"
        path.write_text('{"b": 2, "a": 1}', encoding="utf-8")

        request = build_file_analysis_request(
            file_path=str(path),
            file_conn_id=None,
            prompt="Analyze",
            multi_modal=False,
            max_files=1,
            max_file_size_bytes=1024,
            max_total_size_bytes=1024,
            max_text_chars=500,
            sample_rows=10,
        )

        assert '"a": 1' in request.user_content
        assert '"b": 2' in request.user_content

    def test_text_context_truncation_is_marked(self, tmp_path):
        path = tmp_path / "huge.log"
        path.write_text("line\n" * 400, encoding="utf-8")

        request = build_file_analysis_request(
            file_path=str(path),
            file_conn_id=None,
            prompt="Analyze",
            multi_modal=False,
            max_files=1,
            max_file_size_bytes=16_384,
            max_total_size_bytes=16_384,
            max_text_chars=100,
            sample_rows=10,
        )

        assert request.text_truncated is True
        assert "text_context_truncated=True" in request.user_content

    def test_csv_sample_rows_is_configurable(self, tmp_path):
        path = tmp_path / "metrics.csv"
        path.write_text("name,value\nalpha,1\nbeta,2\ngamma,3\n", encoding="utf-8")

        request = build_file_analysis_request(
            file_path=str(path),
            file_conn_id=None,
            prompt="Analyze",
            multi_modal=False,
            max_files=1,
            max_file_size_bytes=1024,
            max_total_size_bytes=1024,
            max_text_chars=500,
            sample_rows=1,
        )

        assert "alpha, 1" in request.user_content
        assert "beta, 2" not in request.user_content
        assert "gamma, 3" not in request.user_content


class TestFileAnalysisHelpers:
    @pytest.mark.parametrize(
        ("filename", "expected_format", "expected_compression"),
        [
            ("events.csv", "csv", None),
            ("events.csv.gz", "csv", "gzip"),
            ("dashboard.jpg", "jpg", None),
            ("report.pdf", "pdf", None),
            ("app", "log", None),
        ],
    )
    def test_detect_file_format(self, tmp_path, filename, expected_format, expected_compression):
        path = tmp_path / filename
        path.write_bytes(b"content")

        file_format, compression = detect_file_format(ObjectStoragePath(str(path)))

        assert file_format == expected_format
        assert compression == expected_compression

    def test_detect_file_format_rejects_unsupported_compression(self, tmp_path):
        path = tmp_path / "events.csv.zst"
        path.write_bytes(b"content")

        with pytest.raises(LLMFileAnalysisUnsupportedFormatError, match="Compression"):
            detect_file_format(ObjectStoragePath(str(path)))

    @pytest.mark.parametrize("filename", ["sample.parquet.gz", "sample.avro.gz", "sample.png.gz"])
    def test_detect_file_format_rejects_unsupported_gzip_format_combinations(self, tmp_path, filename):
        path = tmp_path / filename
        path.write_bytes(b"content")

        with pytest.raises(LLMFileAnalysisUnsupportedFormatError, match="not supported for"):
            detect_file_format(ObjectStoragePath(str(path)))

    def test_read_raw_bytes_decompresses_gzip(self, tmp_path):
        path = tmp_path / "events.log.gz"
        path.write_bytes(gzip.compress(b"line one\nline two\n"))

        content = _read_raw_bytes(ObjectStoragePath(str(path)), compression="gzip", max_bytes=1_024)

        assert content == b"line one\nline two\n"

    def test_truncate_text_preserves_head_and_tail(self):
        text = "A" * 9_000 + "B" * 3_000

        truncated = _truncate_text(text)

        assert truncated.startswith("A" * 8_000)
        assert truncated.endswith("B" * 1_995)
        assert "\n...\n" in truncated

    def test_truncate_text_with_small_max_chars_returns_prefix_only(self):
        assert _truncate_text("abcdefghij", max_chars=8) == "abcdefgh"

    def test_infer_partitions(self, tmp_path):
        path = tmp_path / "dt=2024-01-15" / "region=us" / "events.csv"
        path.parent.mkdir(parents=True)
        path.write_text("id\n1\n", encoding="utf-8")

        partitions = _infer_partitions(ObjectStoragePath(str(path)))

        assert partitions == ("dt=2024-01-15", "region=us")

    def test_resolve_paths_raises_when_no_files_found(self, tmp_path):
        empty_dir = ObjectStoragePath(str(tmp_path / "missing"))

        with pytest.raises(FileNotFoundError, match="No files found"):
            _resolve_paths(root=empty_dir, max_files=10)


class TestFormatReaders:
    def test_render_parquet_uses_lazy_import(self, tmp_path):
        pyarrow = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "sample.parquet"
        table = pyarrow.Table.from_pylist([{"id": 1}, {"id": 2}])
        pq.write_table(table, path)

        result = _render_parquet(ObjectStoragePath(str(path)), sample_rows=1, max_content_bytes=1_024)

        assert result.estimated_rows == 2
        assert "Schema: id: int64" in result.text
        assert '"id": 1' in result.text
        assert '"id": 2' not in result.text

    def test_render_parquet_does_not_materialize_full_table(self, tmp_path):
        pyarrow = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "sample.parquet"
        table = pyarrow.Table.from_pylist([{"id": 1}, {"id": 2}, {"id": 3}])
        pq.write_table(table, path, row_group_size=1)

        with patch("pyarrow.parquet.ParquetFile.read", autospec=True, side_effect=AssertionError):
            result = _render_parquet(ObjectStoragePath(str(path)), sample_rows=2, max_content_bytes=4_096)

        assert result.estimated_rows == 3
        assert '"id": 1' in result.text
        assert '"id": 2' in result.text
        assert '"id": 3' not in result.text

    def test_render_parquet_enforces_processed_content_limit_before_read(self, tmp_path):
        pyarrow = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "sample.parquet"
        table = pyarrow.Table.from_pylist([{"id": 1}, {"id": 2}])
        pq.write_table(table, path)

        with pytest.raises(LLMFileAnalysisLimitExceededError, match="processed-content limit"):
            _render_parquet(
                ObjectStoragePath(str(path)), sample_rows=1, max_content_bytes=path.stat().st_size - 1
            )

    def test_render_parquet_missing_dependency_raises(self, tmp_path):
        path = tmp_path / "sample.parquet"
        path.write_bytes(b"parquet")
        real_import = builtins.__import__

        def failing_import(name, *args, **kwargs):
            if name in {"pyarrow", "pyarrow.parquet"}:
                raise ImportError("missing pyarrow")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=failing_import):
            with pytest.raises(AirflowOptionalProviderFeatureException, match="parquet"):
                _render_parquet(ObjectStoragePath(str(path)), sample_rows=1, max_content_bytes=1_024)

    def test_render_avro_uses_lazy_import(self, tmp_path):
        fastavro = pytest.importorskip("fastavro")

        path = tmp_path / "sample.avro"
        schema = {
            "type": "record",
            "name": "sample",
            "fields": [{"name": "id", "type": "long"}],
        }
        with path.open("wb") as handle:
            fastavro.writer(handle, schema, [{"id": 1}, {"id": 2}])

        result = _render_avro(ObjectStoragePath(str(path)), sample_rows=1, max_content_bytes=1_024)

        assert result.estimated_rows is None
        assert '"name": "sample"' in result.text
        assert '"id": 1' in result.text
        assert '"id": 2' not in result.text

    def test_render_avro_estimates_rows_when_fully_read(self, tmp_path):
        fastavro = pytest.importorskip("fastavro")

        path = tmp_path / "sample.avro"
        schema = {
            "type": "record",
            "name": "sample",
            "fields": [{"name": "id", "type": "long"}],
        }
        with path.open("wb") as handle:
            fastavro.writer(handle, schema, [{"id": 1}, {"id": 2}])

        result = _render_avro(ObjectStoragePath(str(path)), sample_rows=5, max_content_bytes=1_024)

        assert result.estimated_rows == 2
        assert '"id": 2' in result.text

    def test_render_avro_enforces_processed_content_limit_before_scan(self, tmp_path):
        fastavro = pytest.importorskip("fastavro")

        path = tmp_path / "sample.avro"
        schema = {
            "type": "record",
            "name": "sample",
            "fields": [{"name": "id", "type": "long"}],
        }
        with path.open("wb") as handle:
            fastavro.writer(handle, schema, [{"id": 1}, {"id": 2}])

        with pytest.raises(LLMFileAnalysisLimitExceededError, match="processed-content limit"):
            _render_avro(
                ObjectStoragePath(str(path)), sample_rows=1, max_content_bytes=path.stat().st_size - 1
            )

    def test_render_avro_missing_dependency_raises(self, tmp_path):
        path = tmp_path / "sample.avro"
        path.write_bytes(b"avro")
        real_import = builtins.__import__

        def failing_import(name, *args, **kwargs):
            if name == "fastavro":
                raise ImportError("missing fastavro")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=failing_import):
            with pytest.raises(AirflowOptionalProviderFeatureException, match="avro"):
                _render_avro(ObjectStoragePath(str(path)), sample_rows=1, max_content_bytes=1_024)
