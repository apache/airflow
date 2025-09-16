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

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError

from airflow.exceptions import AirflowException
from airflow.providers.docling.hooks.docling_hook import DoclingHook

TESTS_ROOT = Path(__file__).parent.parent.parent

MOCK_API_RESPONSE = {"status": "success", "content": "Processed text..."}
MOCK_FILE_PATH = TESTS_ROOT / "unit" / "fixtures" / "test_doc.pdf"
MOCK_SOURCE_URL = "https://arxiv.org/pdf/2206.01062"
MOCK_PARAMETERS = {"output_format": "markdown"}


class TestDoclingHook:
    """Tests for DoclingHook."""

    def setup_method(self):
        """Set up the mock response for all tests."""
        self.mock_response = MagicMock()
        self.mock_response.json.return_value = MOCK_API_RESPONSE
        self.mock_response.raise_for_status.return_value = None

    @patch("airflow.providers.docling.hooks.docling_hook.DoclingHook._read_file_content")
    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_process_document_success(self, mock_run, mock_read_file):
        """Tests the successful processing of a local document."""
        # Arrange
        mock_run.return_value = self.mock_response
        mock_read_file.return_value = {
            "content": b"fake bytes",
            "filename": "doc.pdf",
            "format": "application/pdf",
        }
        hook = DoclingHook()

        # Act
        result = hook.process_document(filename=MOCK_FILE_PATH, data=MOCK_PARAMETERS)

        # Assert
        mock_read_file.assert_called_once_with(MOCK_FILE_PATH)
        mock_run.assert_called_once()
        _, kwargs = mock_run.call_args
        assert kwargs["endpoint"] == "/api/v1/convert/file"
        assert kwargs["data"] == MOCK_PARAMETERS
        assert "files" in kwargs
        assert result == MOCK_API_RESPONSE

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_upload_source_success(self, mock_run):
        """Tests the successful conversion of a document from a source URL."""
        # Arrange
        mock_run.return_value = self.mock_response
        hook = DoclingHook()
        expected_payload = {
            "http_sources": [{"url": MOCK_SOURCE_URL}],
            "options": MOCK_PARAMETERS,
        }

        # Act
        result = hook.upload_source(source=MOCK_SOURCE_URL, data=MOCK_PARAMETERS)

        # Assert
        mock_run.assert_called_once_with(endpoint="/api/v1/convert/source", json=expected_payload)
        assert result == MOCK_API_RESPONSE

    @patch("airflow.providers.docling.hooks.docling_hook.DoclingHook._read_file_content")
    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_process_document_api_error(self, mock_run, mock_read_file):
        """Tests API error handling for process_document."""
        # Arrange
        mock_run.return_value.raise_for_status.side_effect = HTTPError("500 Server Error")
        mock_read_file.return_value = {"content": b"", "filename": "test.pdf", "format": "application/pdf"}
        hook = DoclingHook()

        # Act & Assert
        with pytest.raises(AirflowException, match="Docling API request failed"):
            hook.process_document(filename=MOCK_FILE_PATH)

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_upload_source_api_error(self, mock_run):
        """Tests API error handling for upload_source."""
        # Arrange
        mock_run.return_value.raise_for_status.side_effect = HTTPError("404 Not Found")
        hook = DoclingHook()

        # Act & Assert
        with pytest.raises(AirflowException, match="Docling source upload failed"):
            hook.upload_source(source=MOCK_SOURCE_URL)
