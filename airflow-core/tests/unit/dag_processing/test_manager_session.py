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

# airflow-core/tests/unit/dag_processing/test_manager_session.py
from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock

from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager


class TestDagFileProcessorManagerSession:
    @mock.patch("airflow.dag_processing.manager.create_session")
    @mock.patch("airflow.dag_processing.manager.process_parse_results")
    def test_collect_results_uses_explicit_session(self, mock_process_parse_results, mock_create_session):
        manager = DagFileProcessorManager(max_runs=1)

        # Mock session
        mock_session = MagicMock()
        mock_create_session.return_value.__enter__.return_value = mock_session

        # Mock processors
        mock_proc = MagicMock()
        mock_proc.is_ready = True
        mock_proc.parsing_result = MagicMock()

        dag_file = DagFileInfo(bundle_name="testing", rel_path="dag.py", bundle_path="/tmp")

        manager._processors = {dag_file: mock_proc}
        manager._file_stats = {dag_file: MagicMock()}
        manager._bundle_versions = {"testing": "1.0"}

        # Call _collect_results
        manager._collect_results()

        # Verify create_session was called
        mock_create_session.assert_called_once()

        # Check if process_parse_results was called with the session from create_session
        assert mock_process_parse_results.called
        call_args = mock_process_parse_results.call_args
        assert call_args.kwargs["session"] == mock_session
