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

from unittest import mock

import pytest
from sqlalchemy.exc import OperationalError

from airflow.dag_processing.collection import _serialize_dag_capturing_errors


class TestSerializeDagCapturingErrors:
    def test_serialize_dag_capturing_errors_emoji_error(self):
        """
        Test that OperationalError with code 1366 (incorrect string value)
        is caught and returned as an error, rather than raised.
        """
        dag = mock.Mock()
        dag.dag_id = "test_dag_emoji"
        dag.fileloc = "/tmp/test_dag_emoji.py"
        dag.relative_fileloc = "test_dag_emoji.py"

        session = mock.Mock()

        # Create the OperationalError with the specific code 1366
        # MySQLdb.OperationalError args are (code, message)
        orig_error = mock.Mock()
        orig_error.args = (1366, "Incorrect string value: ...")
        # Make sure str(orig_error) returns something useful for the assertion
        orig_error.__str__ = mock.Mock(return_value="(1366, 'Incorrect string value: ...')")
        op_error = OperationalError("INSERT ...", {}, orig_error)

        # Mock session.flush to raise the error (simulating DB write failure)
        session.flush.side_effect = op_error

        # Mock SerializedDagModel.write_dag to succeed (so we reach flush)
        with mock.patch("airflow.models.serialized_dag.SerializedDagModel.write_dag", return_value=True):
            # We also need to mock DagCode.update_source_code if write_dag returns False,
            # but here we return True so it won't be called in the first branch.
            # However, wait, if write_dag returns True, DagCode.update_source_code is NOT called in collection.py

            results = _serialize_dag_capturing_errors(dag, "bundle", session, None)

            assert len(results) == 1
            assert results[0][0] == ("bundle", "test_dag_emoji.py")
            assert "Incorrect string value" in results[0][1]
            # Verify flush was called
            session.flush.assert_called()
            session.rollback.assert_called()

    def test_serialize_dag_capturing_errors_other_operational_error(self):
        """
        Test that other OperationalErrors are still raised.
        """
        dag = mock.Mock()
        dag.dag_id = "test_dag_other"
        dag.fileloc = "/tmp/test_dag_other.py"
        dag.relative_fileloc = "test_dag_other.py"

        session = mock.Mock()

        # Generic OperationalError
        orig_error = mock.Mock()
        orig_error.args = (2006, "MySQL server has gone away")
        op_error = OperationalError("SELECT ...", {}, orig_error)

        session.flush.side_effect = op_error

        with mock.patch("airflow.models.serialized_dag.SerializedDagModel.write_dag", return_value=True):
            with pytest.raises(OperationalError):
                _serialize_dag_capturing_errors(dag, "bundle", session, None)
            session.rollback.assert_called()
