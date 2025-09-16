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

from airflow.providers.docling.operators.docling_process import DoclingConvertOperator
from airflow.providers.docling.operators.docling_source import DoclingConvertSourceOperator

TESTS_ROOT = Path(__file__).parent.parent.parent
CONN_ID = "docling_test_conn"
MOCK_HOOK_RESPONSE = {"status": "success", "content": "Text from operator test"}
MOCK_PARAMETERS = {"output_format": "text"}
MOCK_FILE_PATH = TESTS_ROOT / "unit" / "fixtures" / "test_doc.pdf"


class TestDoclingConvertOperator:
    """Tests for DoclingConvertOperator."""

    @patch("airflow.providers.docling.operators.docling_process.DoclingHook")
    def test_execute(self, mock_docling_hook):
        """Tests the successful execution of the operator."""
        # Arrange
        mock_hook_instance = MagicMock()
        mock_hook_instance.process_document.return_value = MOCK_HOOK_RESPONSE
        mock_docling_hook.return_value = mock_hook_instance
        file_path = MOCK_FILE_PATH

        operator = DoclingConvertOperator(
            task_id="test_docling_task",
            filename=file_path,
            docling_conn_id=CONN_ID,
            parameters=MOCK_PARAMETERS,
        )

        # Act
        result = operator.execute(context=MagicMock())

        # Assert
        mock_docling_hook.assert_called_once_with(http_conn_id=CONN_ID)
        mock_hook_instance.process_document.assert_called_once_with(filename=file_path, data=MOCK_PARAMETERS)
        assert result == MOCK_HOOK_RESPONSE


class TestDoclingConvertSourceOperator:
    """Tests for DoclingConvertSourceOperator."""

    @patch("airflow.providers.docling.operators.docling_source.DoclingHook")
    def test_execute(self, mock_docling_hook):
        """Tests the successful execution of the source operator."""
        # Arrange
        mock_hook_instance = MagicMock()
        mock_hook_instance.upload_source.return_value = MOCK_HOOK_RESPONSE
        mock_docling_hook.return_value = mock_hook_instance
        source_url = "http://example.com/file.pdf"

        operator = DoclingConvertSourceOperator(
            task_id="test_docling_source_task",
            source=source_url,
            docling_conn_id=CONN_ID,
            parameters=MOCK_PARAMETERS,
        )

        # Act
        result = operator.execute(context=MagicMock())

        # Assert
        mock_docling_hook.assert_called_once_with(http_conn_id=CONN_ID)
        mock_hook_instance.upload_source.assert_called_once_with(source=source_url, data=MOCK_PARAMETERS)
        assert result == MOCK_HOOK_RESPONSE
