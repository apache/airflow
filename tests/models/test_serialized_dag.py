# -*- coding: utf-8 -*-
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

import unittest

from airflow import example_dags as example_dags_module
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.models import DagBag
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel as SDM
from airflow.utils import db


# To move it to a shared module.
def make_example_dags(module):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module.__path__[0])
    return dagbag.dags


def clear_db_serialized_dags():
    with db.create_session() as session:
        session.query(SDM).delete()


class SerializedDagModelTest(unittest.TestCase):
    """Unit tests for SerializedDagModel."""

    def setUp(self):
        clear_db_serialized_dags()

    def tearDown(self):
        clear_db_serialized_dags()

    def test_dag_fileloc_hash(self):
        """Verifies the correctness of hashing file path."""
        self.assertEqual(DagCode.dag_fileloc_hash('/airflow/dags/test_dag.py'),
                         33826252060516589)

    def _write_example_dags(self):
        example_dags = make_example_dags(example_dags_module)
        for dag in example_dags.values():
            SDM.write_dag(dag)
        return example_dags

    def test_write_dag(self):
        """DAGs can be written into database."""
        example_dags = self._write_example_dags()

        with db.create_session() as session:
            for dag in example_dags.values():
                self.assertTrue(SDM.has_dag(dag.dag_id))
                result = session.query(
                    SDM.fileloc, SDM.data).filter(SDM.dag_id == dag.dag_id).one()

                self.assertTrue(result.fileloc == dag.full_filepath)
                # Verifies JSON schema.
                SerializedDAG.validate_schema(result.data)

    def test_read_dags(self):
        """DAGs can be read from database."""
        example_dags = self._write_example_dags()
        serialized_dags = SDM.read_all_dags()
        self.assertTrue(len(example_dags) == len(serialized_dags))
        for dag_id, dag in example_dags.items():
            serialized_dag = serialized_dags[dag_id]

            self.assertTrue(serialized_dag.dag_id == dag.dag_id)
            self.assertTrue(set(serialized_dag.task_dict) == set(dag.task_dict))

    def test_remove_dags_by_id(self):
        """DAGs can be removed from database."""
        example_dags_list = list(self._write_example_dags().values())
        # Remove SubDags from the list as they are not stored in DB in a separate row
        # and are directly added in Json blob of the main DAG
        filtered_example_dags_list = [dag for dag in example_dags_list if not dag.is_subdag]
        # Tests removing by dag_id.
        dag_removed_by_id = filtered_example_dags_list[0]
        SDM.remove_dag(dag_removed_by_id.dag_id)
        self.assertFalse(SDM.has_dag(dag_removed_by_id.dag_id))

    def test_remove_dags_by_filepath(self):
        """DAGs can be removed from database."""
        example_dags_list = list(self._write_example_dags().values())
        # Remove SubDags from the list as they are not stored in DB in a separate row
        # and are directly added in Json blob of the main DAG
        filtered_example_dags_list = [dag for dag in example_dags_list if not dag.is_subdag]
        # Tests removing by file path.
        dag_removed_by_file = filtered_example_dags_list[0]
        # remove repeated files for those DAGs that define multiple dags in the same file (set comprehension)
        example_dag_files = list({dag.full_filepath for dag in filtered_example_dags_list})
        example_dag_files.remove(dag_removed_by_file.full_filepath)
        SDM.remove_deleted_dags(example_dag_files)
        self.assertFalse(SDM.has_dag(dag_removed_by_file.dag_id))
