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
"""Unit tests for SerializedDagModel."""
from __future__ import annotations

import unittest
from unittest import mock

from parameterized import parameterized, parameterized_class

from airflow import DAG, example_dags as example_dags_module
from airflow.models import DagBag
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel as SDM
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils.session import create_session
from tests.test_utils.asserts import assert_queries_count


# To move it to a shared module.
def make_example_dags(module):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module.__path__[0])
    return dagbag.dags


def clear_db_serialized_dags():
    with create_session() as session:
        session.query(SDM).delete()


@parameterized_class(
    [
        {"compress_serialized_dags": "False"},
        {"compress_serialized_dags": "True"},
    ]
)
class SerializedDagModelTest(unittest.TestCase):
    """Unit tests for SerializedDagModel."""

    compress_serialized_dags = "False"

    def setUp(self):
        self.patcher = mock.patch(
            "airflow.models.serialized_dag.COMPRESS_SERIALIZED_DAGS", self.compress_serialized_dags
        )
        self.patcher.start()

        clear_db_serialized_dags()

    def tearDown(self):
        self.patcher.stop()
        clear_db_serialized_dags()

    def test_dag_fileloc_hash(self):
        """Verifies the correctness of hashing file path."""
        assert DagCode.dag_fileloc_hash("/airflow/dags/test_dag.py") == 33826252060516589

    def _write_example_dags(self):
        example_dags = make_example_dags(example_dags_module)
        for dag in example_dags.values():
            SDM.write_dag(dag)
        return example_dags

    def test_write_dag(self):
        """DAGs can be written into database"""
        example_dags = self._write_example_dags()

        with create_session() as session:
            for dag in example_dags.values():
                assert SDM.has_dag(dag.dag_id)
                result = session.query(SDM).filter(SDM.dag_id == dag.dag_id).one()

                assert result.fileloc == dag.fileloc
                # Verifies JSON schema.
                SerializedDAG.validate_schema(result.data)

    def test_serialized_dag_is_updated_if_dag_is_changed(self):
        """Test Serialized DAG is updated if DAG is changed"""
        example_dags = make_example_dags(example_dags_module)
        example_bash_op_dag = example_dags.get("example_bash_operator")
        dag_updated = SDM.write_dag(dag=example_bash_op_dag)
        assert dag_updated is True

        with create_session() as session:
            s_dag = session.query(SDM).get(example_bash_op_dag.dag_id)

            # Test that if DAG is not changed, Serialized DAG is not re-written and last_updated
            # column is not updated
            dag_updated = SDM.write_dag(dag=example_bash_op_dag)
            s_dag_1 = session.query(SDM).get(example_bash_op_dag.dag_id)

            assert s_dag_1.dag_hash == s_dag.dag_hash
            assert s_dag.last_updated == s_dag_1.last_updated
            assert dag_updated is False

            # Update DAG
            example_bash_op_dag.tags += ["new_tag"]
            assert set(example_bash_op_dag.tags) == {"example", "example2", "new_tag"}

            dag_updated = SDM.write_dag(dag=example_bash_op_dag)
            s_dag_2 = session.query(SDM).get(example_bash_op_dag.dag_id)

            assert s_dag.last_updated != s_dag_2.last_updated
            assert s_dag.dag_hash != s_dag_2.dag_hash
            assert s_dag_2.data["dag"]["tags"] == ["example", "example2", "new_tag"]
            assert dag_updated is True

    def test_serialized_dag_is_updated_if_processor_subdir_changed(self):
        """Test Serialized DAG is updated if processor_subdir is changed"""
        example_dags = make_example_dags(example_dags_module)
        example_bash_op_dag = example_dags.get("example_bash_operator")
        dag_updated = SDM.write_dag(dag=example_bash_op_dag, processor_subdir="/tmp/test")
        assert dag_updated is True

        with create_session() as session:
            s_dag = session.query(SDM).get(example_bash_op_dag.dag_id)

            # Test that if DAG is not changed, Serialized DAG is not re-written and last_updated
            # column is not updated
            dag_updated = SDM.write_dag(dag=example_bash_op_dag, processor_subdir="/tmp/test")
            s_dag_1 = session.query(SDM).get(example_bash_op_dag.dag_id)

            assert s_dag_1.dag_hash == s_dag.dag_hash
            assert s_dag.last_updated == s_dag_1.last_updated
            assert dag_updated is False
            session.flush()

            # Update DAG
            dag_updated = SDM.write_dag(dag=example_bash_op_dag, processor_subdir="/tmp/other")
            s_dag_2 = session.query(SDM).get(example_bash_op_dag.dag_id)

            assert s_dag.processor_subdir != s_dag_2.processor_subdir
            assert dag_updated is True

    def test_read_dags(self):
        """DAGs can be read from database."""
        example_dags = self._write_example_dags()
        serialized_dags = SDM.read_all_dags()
        assert len(example_dags) == len(serialized_dags)
        for dag_id, dag in example_dags.items():
            serialized_dag = serialized_dags[dag_id]

            assert serialized_dag.dag_id == dag.dag_id
            assert set(serialized_dag.task_dict) == set(dag.task_dict)

    def test_remove_dags_by_id(self):
        """DAGs can be removed from database."""
        example_dags_list = list(self._write_example_dags().values())
        # Remove SubDags from the list as they are not stored in DB in a separate row
        # and are directly added in Json blob of the main DAG
        filtered_example_dags_list = [dag for dag in example_dags_list if not dag.is_subdag]
        # Tests removing by dag_id.
        dag_removed_by_id = filtered_example_dags_list[0]
        SDM.remove_dag(dag_removed_by_id.dag_id)
        assert not SDM.has_dag(dag_removed_by_id.dag_id)

    def test_remove_dags_by_filepath(self):
        """DAGs can be removed from database."""
        example_dags_list = list(self._write_example_dags().values())
        # Remove SubDags from the list as they are not stored in DB in a separate row
        # and are directly added in Json blob of the main DAG
        filtered_example_dags_list = [dag for dag in example_dags_list if not dag.is_subdag]
        # Tests removing by file path.
        dag_removed_by_file = filtered_example_dags_list[0]
        # remove repeated files for those DAGs that define multiple dags in the same file (set comprehension)
        example_dag_files = list({dag.fileloc for dag in filtered_example_dags_list})
        example_dag_files.remove(dag_removed_by_file.fileloc)
        SDM.remove_deleted_dags(example_dag_files)
        assert not SDM.has_dag(dag_removed_by_file.dag_id)

    def test_bulk_sync_to_db(self):
        dags = [
            DAG("dag_1"),
            DAG("dag_2"),
            DAG("dag_3"),
        ]
        with assert_queries_count(10):
            SDM.bulk_sync_to_db(dags)

    @parameterized.expand([({"dag_dependencies": None},), ({},)])
    def test_get_dag_dependencies_default_to_empty(self, dag_dependencies_fields):
        """Test a pre-2.1.0 serialized DAG can deserialize DAG dependencies."""
        example_dags = make_example_dags(example_dags_module)

        with create_session() as session:
            sdms = [SDM(dag) for dag in example_dags.values()]
            # Simulate pre-2.1.0 format.
            for sdm in sdms:
                del sdm.data["dag"]["dag_dependencies"]
                sdm.data["dag"].update(dag_dependencies_fields)
            session.bulk_save_objects(sdms)

        expected_dependencies = {dag_id: [] for dag_id in example_dags}
        assert SDM.get_dag_dependencies() == expected_dependencies
