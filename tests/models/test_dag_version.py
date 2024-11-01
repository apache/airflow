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
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator

from tests_common.test_utils.db import clear_db_dags

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


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
        assert not latest_version.version_name
        assert latest_version.dag_id == dag.dag_id

    @pytest.mark.need_serialized_dag
    def test_writing_dag_version_with_version_name(self, dag_maker, session):
        version_name = "my_version"
        with dag_maker(version_name=version_name) as dag:
            pass

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version_number == 1
        assert latest_version.version_name == version_name
        assert latest_version.dag_id == dag.dag_id

    def test_writing_dag_version_with_changes(self, dag_maker, session):
        """This also tested the get_latest_version method"""
        version_name = "my_version"
        with dag_maker("test1", version_name=version_name) as dag:
            EmptyOperator(task_id="task1")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)
        # Add extra task to change the dag
        with dag_maker("test1", version_name=version_name) as dag2:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        dag2.sync_to_db()
        SerializedDagModel.write_dag(dag2)

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version_number == 2
        assert latest_version.version_name == version_name
        assert 2 == session.scalar(select(func.count()).where(DagVersion.dag_id == dag.dag_id))

    @pytest.mark.need_serialized_dag
    def test_get_version(self, dag_maker, session):
        """The two dags have the same version name and number but different dag ids"""
        version_name = "my_version"
        dag1_id = "test1"
        with dag_maker(dag1_id, version_name=version_name):
            EmptyOperator(task_id="task1")

        with dag_maker("test2", version_name=version_name):
            EmptyOperator(task_id="task1")

        with dag_maker("test3"):
            EmptyOperator(task_id="task1")

        version = DagVersion.get_version(dag1_id)
        assert version.version_number == 1
        assert version.version_name == version_name
        assert version.dag_id == dag1_id
        assert version.version == "my_version-1"

    def test_get_latest_dag_versions(self, dag_maker, session):
        # first dag
        version_name = "test_v"
        with dag_maker("dag1", version_name=version_name) as dag:
            EmptyOperator(task_id="task1")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)
        with dag_maker("dag1", version_name=version_name) as dag:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)
        # second dag
        version_name2 = "test_v2"
        with dag_maker("dag2", version_name=version_name2) as dag:
            EmptyOperator(task_id="task1")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)
        with dag_maker("dag2", version_name=version_name2) as dag:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)

        # Total versions should be 4
        assert session.scalar(select(func.count()).select_from(DagVersion)) == 4

        latest_versions_for_the_dags = {f"{version_name}-2", f"{version_name2}-2"}
        latest_versions = DagVersion.get_latest_dag_versions(["dag1", "dag2"])
        assert latest_versions_for_the_dags == {x.version for x in latest_versions}

    @pytest.mark.need_serialized_dag
    def test_version_property(self, dag_maker):
        version_name = "my_version"
        with dag_maker("test1", version_name=version_name) as dag:
            pass

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version == f"{version_name}-1"

    @pytest.mark.need_serialized_dag
    def test_version_property_with_null_version_name(self, dag_maker):
        with dag_maker("test1") as dag:
            pass

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version == "1"
