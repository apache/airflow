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

from unittest import mock

import pendulum
import pytest

import airflow.example_dags as example_dags_module
from airflow.datasets import Dataset
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel as SDM
from airflow.operators.bash import BashOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.settings import json
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.session import create_session
from tests.test_utils import db
from tests.test_utils.asserts import assert_queries_count

pytestmark = pytest.mark.db_test


# To move it to a shared module.
def make_example_dags(module):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module.__path__[0])
    return dagbag.dags


class TestSerializedDagModel:
    """Unit tests for SerializedDagModel."""

    @pytest.fixture(
        autouse=True,
        params=[
            pytest.param(False, id="raw-serialized_dags"),
            pytest.param(True, id="compress-serialized_dags"),
        ],
    )
    def setup_test_cases(self, request, monkeypatch):
        db.clear_db_serialized_dags()
        with mock.patch("airflow.models.serialized_dag.COMPRESS_SERIALIZED_DAGS", request.param):
            yield
        db.clear_db_serialized_dags()

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
            s_dag = session.get(SDM, example_bash_op_dag.dag_id)

            # Test that if DAG is not changed, Serialized DAG is not re-written and last_updated
            # column is not updated
            dag_updated = SDM.write_dag(dag=example_bash_op_dag)
            s_dag_1 = session.get(SDM, example_bash_op_dag.dag_id)

            assert s_dag_1.dag_hash == s_dag.dag_hash
            assert s_dag.last_updated == s_dag_1.last_updated
            assert dag_updated is False

            # Update DAG
            example_bash_op_dag.tags += ["new_tag"]
            assert set(example_bash_op_dag.tags) == {"example", "example2", "new_tag"}

            dag_updated = SDM.write_dag(dag=example_bash_op_dag)
            s_dag_2 = session.get(SDM, example_bash_op_dag.dag_id)

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
            s_dag = session.get(SDM, example_bash_op_dag.dag_id)

            # Test that if DAG is not changed, Serialized DAG is not re-written and last_updated
            # column is not updated
            dag_updated = SDM.write_dag(dag=example_bash_op_dag, processor_subdir="/tmp/test")
            s_dag_1 = session.get(SDM, example_bash_op_dag.dag_id)

            assert s_dag_1.dag_hash == s_dag.dag_hash
            assert s_dag.last_updated == s_dag_1.last_updated
            assert dag_updated is False
            session.flush()

            # Update DAG
            dag_updated = SDM.write_dag(dag=example_bash_op_dag, processor_subdir="/tmp/other")
            s_dag_2 = session.get(SDM, example_bash_op_dag.dag_id)

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
        SDM.remove_deleted_dags(example_dag_files, processor_subdir="/tmp/test")
        assert not SDM.has_dag(dag_removed_by_file.dag_id)

    def test_bulk_sync_to_db(self):
        dags = [
            DAG("dag_1"),
            DAG("dag_2"),
            DAG("dag_3"),
        ]
        with assert_queries_count(10):
            SDM.bulk_sync_to_db(dags)

    @pytest.mark.parametrize("dag_dependencies_fields", [{"dag_dependencies": None}, {}])
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

    def test_order_of_deps_is_consistent(self):
        """
        Previously the 'dag_dependencies' node in serialized dag was converted to list from set.
        This caused the order, and thus the hash value, to be unreliable, which could produce
        excessive dag parsing.
        """
        first_dag_hash = None
        for r in range(10):
            with DAG(
                dag_id="example",
                start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
                schedule=[
                    Dataset("1"),
                    Dataset("2"),
                    Dataset("3"),
                    Dataset("4"),
                    Dataset("5"),
                ],
            ) as dag6:
                BashOperator(
                    task_id="any",
                    outlets=[Dataset("0*"), Dataset("6*")],
                    bash_command="sleep 5",
                )
            deps_order = [x["dependency_id"] for x in SerializedDAG.serialize_dag(dag6)["dag_dependencies"]]
            # in below assert, 0 and 6 both come at end because "source" is different for them and source
            # is the first field in DagDependency class
            assert deps_order == ["1", "2", "3", "4", "5", "0*", "6*"]

            # for good measure, let's check that the dag hash is consistent
            dag_json = json.dumps(SerializedDAG.to_dict(dag6), sort_keys=True).encode("utf-8")
            this_dag_hash = md5(dag_json).hexdigest()

            # set first dag hash on first pass
            if first_dag_hash is None:
                first_dag_hash = this_dag_hash

            # dag hash should not change without change in structure (we're in a loop)
            assert this_dag_hash == first_dag_hash
