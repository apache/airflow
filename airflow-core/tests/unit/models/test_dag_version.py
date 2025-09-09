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

import pytest
from sqlalchemy import func, select

from airflow.models.dag_version import DagVersion
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dags

pytestmark = pytest.mark.db_test


class TestDagVersion:
    def setup_method(self):
        clear_db_dags()

    def teardown_method(self):
        clear_db_dags()

    @pytest.mark.need_serialized_dag
    def test_writing_dag_version(self, dag_maker, session):
        with dag_maker("test_writing_dag_version") as dag:
            pass

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version_number == 1
        assert latest_version.dag_id == dag.dag_id

    def test_writing_dag_version_with_changes(self, dag_maker, session):
        """This also tested the get_latest_version method"""
        with dag_maker("test1") as dag:
            EmptyOperator(task_id="task1")
        sync_dag_to_db(dag)
        dag_maker.create_dagrun()
        # Add extra task to change the dag
        with dag_maker("test1") as dag2:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        sync_dag_to_db(dag2)
        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version_number == 2
        assert session.scalar(select(func.count()).where(DagVersion.dag_id == dag.dag_id)) == 2

    @pytest.mark.need_serialized_dag
    def test_get_version(self, dag_maker, session):
        """The two dags have the same version name and number but different dag ids"""
        dag1_id = "test1"
        with dag_maker(dag1_id):
            EmptyOperator(task_id="task1")

        with dag_maker("test2"):
            EmptyOperator(task_id="task1")

        with dag_maker("test3"):
            EmptyOperator(task_id="task1")

        version = DagVersion.get_version(dag1_id)
        assert version.version_number == 1
        assert version.dag_id == dag1_id
        assert version.version == f"{dag1_id}-1"

    @pytest.mark.need_serialized_dag
    def test_version_property(self, dag_maker):
        with dag_maker("test1") as dag:
            pass

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version == f"{dag.dag_id}-1"
