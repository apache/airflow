#
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

from unittest.mock import patch

import pytest

import airflow.example_dags as example_dags_module
from airflow.exceptions import AirflowException
from airflow.models import DagBag
from airflow.models.dag import DAG
from airflow.models.dag_version import DagVersion
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel as SDM

# To move it to a shared module.
from airflow.utils.file import open_maybe_zipped
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_dag_code, clear_db_dags

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


def make_example_dags(module):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module.__path__[0])
    DAG.bulk_write_to_db(dagbag.dags.values())
    return dagbag.dags


class TestDagCode:
    """Unit tests for DagCode."""

    def setup_method(self):
        clear_db_dags()
        clear_db_dag_code()

    def teardown_method(self):
        clear_db_dags()
        clear_db_dag_code()

    def _write_two_example_dags(self):
        example_dags = make_example_dags(example_dags_module)
        bash_dag = example_dags["example_bash_operator"]
        dag_version = DagVersion.get_latest_version("example_bash_operator")
        DagCode(dag_version, bash_dag.fileloc).sync_to_db()
        xcom_dag = example_dags["example_xcom"]
        dag_version = DagVersion.get_latest_version("example_xcom")
        DagCode(dag_version, xcom_dag.fileloc).sync_to_db()
        return [bash_dag, xcom_dag]

    def _write_example_dags(self):
        example_dags = make_example_dags(example_dags_module)
        for dag in example_dags.values():
            SDM.write_dag(dag)
        return example_dags

    def test_write_to_db(self):
        """Dg code can be written into database."""
        example_dags = self._write_example_dags()

        self._compare_example_dags(example_dags)

    @patch.object(DagCode, "dag_fileloc_hash")
    def test_detecting_duplicate_key(self, mock_hash):
        """Dag code detects duplicate key."""
        mock_hash.return_value = 0

        with pytest.raises(AirflowException):
            self._write_two_example_dags()

    def _compare_example_dags(self, example_dags):
        with create_session() as session:
            for dag in example_dags.values():
                assert DagCode.has_dag(dag.fileloc)
                dag_fileloc_hash = DagCode.dag_fileloc_hash(dag.fileloc)
                result = (
                    session.query(DagCode.fileloc, DagCode.fileloc_hash, DagCode.source_code)
                    .filter(DagCode.fileloc == dag.fileloc)
                    .filter(DagCode.fileloc_hash == dag_fileloc_hash)
                    .order_by(DagCode.last_updated.desc())
                    .limit(1)
                    .one()
                )

                assert result.fileloc == dag.fileloc
                with open_maybe_zipped(dag.fileloc, "r") as source:
                    source_code = source.read()
                assert result.source_code == source_code

    def test_code_can_be_read_when_no_access_to_file(self):
        """
        Test that code can be retrieved from DB when you do not have access to Code file.
        Source Code should at least exist in one of DB or File.
        """
        example_dag = make_example_dags(example_dags_module).get("example_bash_operator")
        SDM.write_dag(example_dag)

        # Mock that there is no access to the Dag File
        with patch("airflow.models.dagcode.open_maybe_zipped") as mock_open:
            mock_open.side_effect = FileNotFoundError
            dag_code = DagCode.get_code_by_fileloc(example_dag.fileloc)

            for test_string in ["example_bash_operator", "also_run_this", "run_this_last"]:
                assert test_string in dag_code

    def test_db_code_created_on_dag_file_change(self, file_updater, session):
        """Test new DagCode is created in DB when DAG file is changed"""
        example_dag = make_example_dags(example_dags_module).get("example_bash_operator")
        SDM.write_dag(example_dag)

        result = (
            session.query(DagCode)
            .filter(DagCode.fileloc == example_dag.fileloc)
            .order_by(DagCode.last_updated.desc())
            .limit(1)
            .one()
        )

        assert result.fileloc == example_dag.fileloc
        assert result.source_code is not None

        with file_updater(example_dag.fileloc):
            example_dag = make_example_dags(example_dags_module).get("example_bash_operator")
            SDM.write_dag(example_dag)
            with patch("airflow.models.dagcode.DagCode._get_code_from_file") as mock_code:
                mock_code.return_value = "# dummy code"
                SDM.write_dag(example_dag)

                new_result = (
                    session.query(DagCode)
                    .filter(DagCode.fileloc == example_dag.fileloc)
                    .order_by(DagCode.last_updated.desc())
                    .limit(1)
                    .one()
                )

                assert new_result.fileloc == example_dag.fileloc
                assert new_result.source_code != result.source_code
                assert new_result.last_updated > result.last_updated

    def test_has_dag(self, dag_maker):
        """Test has_dag method."""
        with dag_maker("test_has_dag") as dag:
            pass
        dag.sync_to_db()
        SDM.write_dag(dag)

        with dag_maker() as dag2:
            pass
        dag2.sync_to_db()
        SDM.write_dag(dag2)

        assert DagCode.has_dag(dag.fileloc)
