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
import unittest
from datetime import timedelta

import pendulum
import six
from mock import patch

from airflow import AirflowException, example_dags as example_dags_module
from airflow.models import DagBag
from airflow.models.dagcode import DagCode
from airflow.utils.db import create_session
# To move it to a shared module.
from airflow.utils.file import open_maybe_zipped
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dag_code


def make_example_dags(module):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module.__path__[0])
    return dagbag.dags


class TestDagCode(unittest.TestCase):
    """Unit tests for DagCode."""

    def setUp(self):
        clear_db_dag_code()

    def tearDown(self):
        clear_db_dag_code()

    def _write_two_example_dags(self):
        example_dags = make_example_dags(example_dags_module)
        bash_dag = example_dags['example_bash_operator']
        DagCode(bash_dag.fileloc).sync_to_db()
        xcom_dag = example_dags['example_xcom']
        DagCode(xcom_dag.fileloc).sync_to_db()
        return [bash_dag, xcom_dag]

    @conf_vars({('core', 'store_dag_code'): 'True'})
    @patch("airflow.models.dag.settings.STORE_DAG_CODE", True)
    def _write_example_dags(self):
        example_dags = make_example_dags(example_dags_module)
        for dag in example_dags.values():
            dag.sync_to_db()
        return example_dags

    def test_sync_to_db(self):
        """Dg code can be written into database."""
        example_dags = self._write_example_dags()

        self._compare_example_dags(example_dags)

    @conf_vars({('core', 'store_dag_code'): 'True'})
    def test_bulk_sync_to_db(self):
        """Dg code can be bulk written into database."""
        example_dags = make_example_dags(example_dags_module)
        files = [dag.fileloc for dag in example_dags.values()]
        with create_session() as session:
            DagCode.bulk_sync_to_db(files, session=session)
            session.commit()

        self._compare_example_dags(example_dags)

    @conf_vars({('core', 'store_dag_code'): 'True'})
    def test_bulk_sync_to_db_half_files(self):
        """Dg code can be bulk written into database."""
        example_dags = make_example_dags(example_dags_module)
        files = [dag.fileloc for dag in example_dags.values()]
        half_files = files[:int(len(files) / 2)]
        with create_session() as session:
            DagCode.bulk_sync_to_db(half_files, session=session)
            session.commit()
        with create_session() as session:
            DagCode.bulk_sync_to_db(files, session=session)
            session.commit()

        self._compare_example_dags(example_dags)

    @patch.object(DagCode, 'dag_fileloc_hash')
    def test_detecting_duplicate_key(self, mock_hash):
        """Dag code detects duplicate key."""
        mock_hash.return_value = 0

        with self.assertRaises(AirflowException):
            self._write_two_example_dags()

    def _compare_example_dags(self, example_dags):
        with create_session() as session:
            for dag in example_dags.values():
                if dag.is_subdag:
                    dag.fileloc = dag.parent_dag.fileloc
                self.assertTrue(DagCode.has_dag(dag.fileloc))
                dag_fileloc_hash = DagCode.dag_fileloc_hash(dag.fileloc)
                result = session.query(
                    DagCode.fileloc, DagCode.fileloc_hash, DagCode.source_code) \
                    .filter(DagCode.fileloc == dag.fileloc) \
                    .filter(DagCode.fileloc_hash == dag_fileloc_hash) \
                    .one()

                self.assertEqual(result.fileloc, dag.fileloc)
                with open_maybe_zipped(dag.fileloc, 'r') as source:
                    source_code = source.read()
                self.assertEqual(result.source_code, source_code)

    @conf_vars({('core', 'store_dag_code'): 'True'})
    @patch("airflow.models.dag.settings.STORE_DAG_CODE", True)
    @patch("airflow.models.dagcode.STORE_DAG_CODE", True)
    def test_code_can_be_read_when_no_access_to_file(self):
        """
        Test that code can be retrieved from DB when you do not have access to Code file.

        Source Code should atleast exist in one of DB or File.
        """
        example_dag = make_example_dags(example_dags_module).get('example_bash_operator')
        example_dag.sync_to_db()

        # Mock that there is no access to the Dag File
        with patch('airflow.models.dagcode.open_maybe_zipped') as mock_open:
            if six.PY2:
                mock_open.side_effect = IOError
            else:
                mock_open.side_effect = FileNotFoundError
            dag_code = DagCode.get_code_by_fileloc(example_dag.fileloc)

            for test_string in ['example_bash_operator', 'also_run_this', 'run_this_last']:
                self.assertIn(test_string, dag_code)

    @conf_vars({('core', 'store_dag_code'): 'True'})
    @patch("airflow.models.dag.settings.STORE_DAG_CODE", True)
    def test_db_code_updated_on_dag_file_change(self):
        """Test if DagCode is updated in DB when DAG file is changed"""
        example_dag = make_example_dags(example_dags_module).get('example_bash_operator')
        example_dag.sync_to_db()

        with create_session() as session:
            result = session.query(DagCode) \
                .filter(DagCode.fileloc == example_dag.fileloc) \
                .one()

            self.assertEqual(result.fileloc, example_dag.fileloc)
            self.assertIsNotNone(result.source_code)

        with patch('airflow.models.dagcode.os.path.getmtime') as mock_mtime:
            if six.PY2:
                mock_mtime.return_value = pendulum.instance(
                    result.last_updated + timedelta(seconds=1)).timestamp()
            else:
                mock_mtime.return_value = (result.last_updated + timedelta(seconds=1)).timestamp()

            with patch('airflow.models.dagcode.DagCode._get_code_from_file') as mock_code:
                mock_code.return_value = "# dummy code"
                example_dag.sync_to_db()

                with create_session() as session:
                    new_result = session.query(DagCode) \
                        .filter(DagCode.fileloc == example_dag.fileloc) \
                        .one()

                    self.assertEqual(new_result.fileloc, example_dag.fileloc)
                    self.assertEqual(new_result.source_code, "# dummy code")
                    self.assertGreater(new_result.last_updated, result.last_updated)
