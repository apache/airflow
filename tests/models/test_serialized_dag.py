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
from sqlalchemy import func, select

import airflow.example_dags as example_dags_module
from airflow.decorators import task as task_decorator
from airflow.models.dag import DAG
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DagBag
from airflow.models.serialized_dag import SerializedDagModel as SDM
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.asset import Asset
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.settings import json
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils import db
from tests_common.test_utils.asserts import assert_queries_count

pytestmark = pytest.mark.db_test


# To move it to a shared module.
def make_example_dags(module):
    """Loads DAGs from a module for test."""
    from airflow.models.dagbundle import DagBundleModel
    from airflow.utils.session import create_session

    with create_session() as session:
        if session.query(DagBundleModel).filter(DagBundleModel.name == "testing").count() == 0:
            testing = DagBundleModel(name="testing")
            session.add(testing)

    dagbag = DagBag(module.__path__[0])
    DAG.bulk_write_to_db("testing", None, dagbag.dags.values())
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
        db.clear_db_dags()
        db.clear_db_runs()
        db.clear_db_serialized_dags()
        with mock.patch("airflow.models.serialized_dag.COMPRESS_SERIALIZED_DAGS", request.param):
            yield
        db.clear_db_serialized_dags()

    def _write_example_dags(self):
        example_dags = make_example_dags(example_dags_module)
        for dag in example_dags.values():
            SDM.write_dag(dag, bundle_name="testing")
        return example_dags

    def test_write_dag(self, testing_dag_bundle):
        """DAGs can be written into database"""
        example_dags = self._write_example_dags()

        with create_session() as session:
            for dag in example_dags.values():
                assert SDM.has_dag(dag.dag_id)
                result = session.query(SDM).filter(SDM.dag_id == dag.dag_id).one()

                assert result.dag_version.dag_code.fileloc == dag.fileloc
                # Verifies JSON schema.
                SerializedDAG.validate_schema(result.data)

    def test_write_dag_when_python_callable_name_changes(self, dag_maker, session):
        def my_callable():
            pass

        with dag_maker("dag1") as dag:
            PythonOperator(task_id="task1", python_callable=my_callable)
        dag.sync_to_db()
        SDM.write_dag(dag, bundle_name="dag_maker")
        dag_maker.create_dagrun(run_id="test1")
        with dag_maker("dag1") as dag:
            PythonOperator(task_id="task1", python_callable=lambda x: None)
        SDM.write_dag(dag, bundle_name="dag_maker")
        dag_maker.create_dagrun(run_id="test2", logical_date=pendulum.datetime(2025, 1, 1))
        assert len(session.query(DagVersion).all()) == 2

        with dag_maker("dag2") as dag:

            @task_decorator
            def my_callable():
                pass

            my_callable()
        dag.sync_to_db()
        SDM.write_dag(dag, bundle_name="dag_maker")
        dag_maker.create_dagrun(run_id="test3", logical_date=pendulum.datetime(2025, 1, 2))
        with dag_maker("dag2") as dag:

            @task_decorator
            def my_callable2():
                pass

            my_callable2()
        SDM.write_dag(dag, bundle_name="testing")

        assert len(session.query(DagVersion).all()) == 4

    def test_serialized_dag_is_updated_if_dag_is_changed(self, testing_dag_bundle):
        """Test Serialized DAG is updated if DAG is changed"""
        example_dags = make_example_dags(example_dags_module)
        example_bash_op_dag = example_dags.get("example_bash_operator")
        dag_updated = SDM.write_dag(dag=example_bash_op_dag, bundle_name="testing")
        assert dag_updated is True
        example_bash_op_dag.create_dagrun(
            run_id="test1",
            run_after=pendulum.datetime(2025, 1, 1, tz="UTC"),
            state=DagRunState.QUEUED,
            triggered_by=DagRunTriggeredByType.TEST,
            run_type=DagRunType.MANUAL,
        )

        s_dag = SDM.get(example_bash_op_dag.dag_id)

        # Test that if DAG is not changed, Serialized DAG is not re-written and last_updated
        # column is not updated
        dag_updated = SDM.write_dag(dag=example_bash_op_dag, bundle_name="testing")
        s_dag_1 = SDM.get(example_bash_op_dag.dag_id)

        assert s_dag_1.dag_hash == s_dag.dag_hash
        assert s_dag.created_at == s_dag_1.created_at
        assert dag_updated is False

        # Update DAG
        example_bash_op_dag.tags.add("new_tag")
        assert example_bash_op_dag.tags == {"example", "example2", "new_tag"}

        dag_updated = SDM.write_dag(dag=example_bash_op_dag, bundle_name="testing")
        s_dag_2 = SDM.get(example_bash_op_dag.dag_id)

        assert s_dag.created_at != s_dag_2.created_at
        assert s_dag.dag_hash != s_dag_2.dag_hash
        assert s_dag_2.data["dag"]["tags"] == ["example", "example2", "new_tag"]
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

    def test_read_all_dags_only_picks_the_latest_serdags(self, session):
        example_dags = self._write_example_dags()
        serialized_dags = SDM.read_all_dags()
        assert len(example_dags) == len(serialized_dags)

        dag = example_dags.get("example_bash_operator")
        dag.create_dagrun(
            run_id="test1",
            run_after=pendulum.datetime(2025, 1, 1, tz="UTC"),
            state=DagRunState.QUEUED,
            triggered_by=DagRunTriggeredByType.TEST,
            run_type=DagRunType.MANUAL,
        )
        dag.doc_md = "new doc string"
        SDM.write_dag(dag, bundle_name="testing")
        serialized_dags2 = SDM.read_all_dags()
        sdags = session.query(SDM).all()
        # assert only the latest SDM is returned
        assert len(sdags) != len(serialized_dags2)

    def test_bulk_sync_to_db(self, testing_dag_bundle):
        dags = [
            DAG("dag_1", schedule=None),
            DAG("dag_2", schedule=None),
            DAG("dag_3", schedule=None),
        ]
        DAG.bulk_write_to_db("testing", None, dags)
        # we also write to dag_version and dag_code tables
        # in dag_version.
        with assert_queries_count(24):
            SDM.bulk_sync_to_db(dags, bundle_name="testing")

    def test_order_of_dag_params_is_stable(self):
        """
        This asserts that we have logic in place which guarantees the order
        of the params is maintained - even if the backend (e.g. MySQL) mutates
        the serialized DAG JSON.
        """
        example_dags = make_example_dags(example_dags_module)
        example_params_trigger_ui = example_dags.get("example_params_trigger_ui")
        before = list(example_params_trigger_ui.params.keys())

        SDM.write_dag(example_params_trigger_ui, bundle_name="testing")
        retrieved_dag = SDM.get_dag("example_params_trigger_ui")
        after = list(retrieved_dag.params.keys())

        assert before == after

    def test_order_of_deps_is_consistent(self):
        """
        Previously the 'dag_dependencies' node in serialized dag was converted to list from set.
        This caused the order, and thus the hash value, to be unreliable, which could produce
        excessive dag parsing.
        """
        first_dag_hash = None
        for _ in range(10):
            with DAG(
                dag_id="example",
                start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
                schedule=[
                    Asset(uri="test://asset1", name="1"),
                    Asset(uri="test://asset2", name="2"),
                    Asset(uri="test://asset3", name="3"),
                    Asset(uri="test://asset4", name="4"),
                    Asset(uri="test://asset5", name="5"),
                ],
            ) as dag6:
                BashOperator(
                    task_id="any",
                    outlets=[Asset(uri="test://asset0", name="0*"), Asset(uri="test://asset6", name="6*")],
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

    def test_example_dag_hashes_are_always_consistent(self, session):
        """
        This test asserts that the hashes of the example dags are always consistent.
        """

        def get_hash_set():
            example_dags = self._write_example_dags()
            ordered_example_dags = dict(sorted(example_dags.items()))
            hashes = set()
            for dag_id in ordered_example_dags.keys():
                smd = session.execute(select(SDM.dag_hash).where(SDM.dag_id == dag_id)).one()
                hashes.add(smd.dag_hash)
            return hashes

        first_hashes = get_hash_set()
        # assert that the hashes are the same
        assert first_hashes == get_hash_set()

    def test_get_latest_serdag_versions(self, dag_maker, session):
        # first dag
        with dag_maker("dag1") as dag:
            EmptyOperator(task_id="task1")
        dag.sync_to_db()
        SDM.write_dag(dag, bundle_name="testing")
        dag_maker.create_dagrun()
        with dag_maker("dag1") as dag:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        dag.sync_to_db()
        SDM.write_dag(dag, bundle_name="testing")
        dag_maker.create_dagrun(run_id="test2", logical_date=pendulum.datetime(2025, 1, 1))
        # second dag
        with dag_maker("dag2") as dag:
            EmptyOperator(task_id="task1")
        dag.sync_to_db()
        SDM.write_dag(dag, bundle_name="testing")
        dag_maker.create_dagrun(run_id="test3", logical_date=pendulum.datetime(2025, 1, 2))
        with dag_maker("dag2") as dag:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        dag.sync_to_db()
        SDM.write_dag(dag, bundle_name="testing")

        # Total serdags should be 4
        assert session.scalar(select(func.count()).select_from(SDM)) == 4

        latest_versions = SDM.get_latest_serialized_dags(dag_ids=["dag1", "dag2"], session=session)
        assert len(latest_versions) == 2

    def test_new_dag_versions_are_not_created_if_no_dagruns(self, dag_maker, session):
        with dag_maker("dag1") as dag:
            PythonOperator(task_id="task1", python_callable=lambda: None)
        dag.sync_to_db()
        SDM.write_dag(dag, bundle_name="testing")
        assert session.query(SDM).count() == 1
        sdm1 = SDM.get(dag.dag_id, session=session)
        dag_hash = sdm1.dag_hash
        created_at = sdm1.created_at
        last_updated = sdm1.last_updated
        # new task
        PythonOperator(task_id="task2", python_callable=lambda: None, dag=dag)
        SDM.write_dag(dag, bundle_name="testing")
        sdm2 = SDM.get(dag.dag_id, session=session)

        assert sdm2.dag_hash != dag_hash  # first recorded serdag
        assert sdm2.created_at == created_at
        assert sdm2.last_updated != last_updated
        assert session.query(DagVersion).count() == 1
        assert session.query(SDM).count() == 1

    def test_new_dag_versions_are_created_if_there_is_a_dagrun(self, dag_maker, session):
        with dag_maker("dag1") as dag:
            PythonOperator(task_id="task1", python_callable=lambda: None)
        dag.sync_to_db()
        SDM.write_dag(dag, bundle_name="testing")
        dag_maker.create_dagrun(run_id="test3", logical_date=pendulum.datetime(2025, 1, 2))
        assert session.query(SDM).count() == 1
        assert session.query(DagVersion).count() == 1
        # new task
        PythonOperator(task_id="task2", python_callable=lambda: None, dag=dag)
        SDM.write_dag(dag, bundle_name="testing")

        assert session.query(DagVersion).count() == 2
        assert session.query(SDM).count() == 2
