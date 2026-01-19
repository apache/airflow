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

import logging
import os

import pytest

from airflow.dag_processing.dagbag import DagBag

pytestmark = pytest.mark.db_test


class TestDuplicateDagIdWarning:
    """Test that DagBag warns when duplicate dag_id is detected."""

    def test_duplicate_dag_id_emits_warning(self, tmp_path, caplog):
        """
        Test that when two DAG files define the same dag_id,
        DagBag emits a WARNING log with both file paths.
        """
        # Create first DAG file with dag_id = "duplicate_dag"
        dag_a = tmp_path / "dag_a.py"
        dag_a.write_text(
            """
from airflow.sdk import DAG
from datetime import datetime

with DAG(
    dag_id="duplicate_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
):
    pass
"""
        )

        # Create second DAG file with the same dag_id = "duplicate_dag"
        dag_b = tmp_path / "dag_b.py"
        dag_b.write_text(
            """
from airflow.sdk import DAG
from datetime import datetime

with DAG(
    dag_id="duplicate_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
):
    pass
"""
        )

        # Parse the DAG folder with caplog to capture warnings
        with caplog.at_level(logging.WARNING):
            dagbag = DagBag(dag_folder=os.fspath(tmp_path), include_examples=False)

        # Assert that only one DAG remains in the bag (the second one overrides the first)
        assert len(dagbag.dags) == 1
        assert "duplicate_dag" in dagbag.dags

        # Assert that a WARNING was logged
        warning_logs = [record for record in caplog.records if record.levelname == "WARNING"]
        assert len(warning_logs) > 0

        # Find the duplicate DAG ID warning
        duplicate_warning = None
        for record in warning_logs:
            if "Duplicate DAG ID" in record.message and "duplicate_dag" in record.message:
                duplicate_warning = record
                break

        assert duplicate_warning is not None, "Expected duplicate DAG ID warning not found"

        # Verify the warning message contains the expected information
        warning_message = duplicate_warning.message
        assert "duplicate_dag" in warning_message
        assert "dag_a.py" in warning_message
        assert "dag_b.py" in warning_message
        assert "Original DAG file" in warning_message or "New DAG file" in warning_message
