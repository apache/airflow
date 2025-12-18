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

import logging
from unittest import mock

import pendulum
import pytest
from sqlalchemy import func, select, update

import airflow.example_dags as example_dags_module
from airflow.dag_processing.dagbag import DagBag
from airflow.models.asset import AssetActive, AssetAliasModel, AssetModel
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.serialized_dag import SerializedDagModel as SDM
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset, AssetAlias, task as task_decorator
from airflow.serialization.dag_dependency import DagDependency
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
from airflow.settings import json
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils import db
from tests_common.test_utils.dag import create_scheduler_dag, sync_dag_to_db

logger = logging.getLogger(__name__)

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

    dags = [LazyDeserializedDAG(data=DagSerialization.to_dict(dag)) for dag in dagbag.dags.values()]
    SerializedDAG.bulk_write_to_db("testing", None, dags)
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
            SDM.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name="testing")
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
                DagSerialization.validate_schema(result.data)

    def test_write_dag_when_python_callable_name_changes(self, dag_maker, session):
        def my_callable():
            pass

        with dag_maker("dag1"):
            PythonOperator(task_id="task1", python_callable=my_callable)
        dag_maker.create_dagrun(run_id="test1")

        with dag_maker("dag1"):
            PythonOperator(task_id="task1", python_callable=lambda x: None)
        dag_maker.create_dagrun(run_id="test2", logical_date=pendulum.datetime(2025, 1, 1))
        assert len(session.query(DagVersion).all()) == 2

        with dag_maker("dag2"):

            @task_decorator
            def my_callable():
                pass

            my_callable()
        dag_maker.create_dagrun(run_id="test3", logical_date=pendulum.datetime(2025, 1, 2))

        with dag_maker("dag2"):

            @task_decorator
            def my_callable2():
                pass

            my_callable2()
        assert len(session.query(DagVersion).all()) == 4

    def test_serialized_dag_is_updated_if_dag_is_changed(self, testing_dag_bundle):
        """Test Serialized DAG is updated if DAG is changed"""
        example_dags = make_example_dags(example_dags_module)
        example_bash_op_dag = example_dags.get("example_bash_operator")
        dag_updated = SDM.write_dag(
            dag=LazyDeserializedDAG.from_dag(example_bash_op_dag),
            bundle_name="testing",
        )
        assert dag_updated is True

        s_dag = SDM.get(example_bash_op_dag.dag_id)
        s_dag.dag.create_dagrun(
            run_id="test1",
            run_after=pendulum.datetime(2025, 1, 1, tz="UTC"),
            state=DagRunState.QUEUED,
            triggered_by=DagRunTriggeredByType.TEST,
            run_type=DagRunType.MANUAL,
        )

        # Test that if DAG is not changed, Serialized DAG is not re-written and last_updated
        # column is not updated
        dag_updated = SDM.write_dag(
            dag=LazyDeserializedDAG.from_dag(example_bash_op_dag),
            bundle_name="testing",
        )
        s_dag_1 = SDM.get(example_bash_op_dag.dag_id)

        assert s_dag_1.dag_hash == s_dag.dag_hash
        assert s_dag.created_at == s_dag_1.created_at
        assert dag_updated is False

        # Update DAG
        example_bash_op_dag.tags.add("new_tag")
        assert example_bash_op_dag.tags == {"example", "example2", "new_tag"}

        dag_updated = SDM.write_dag(
            dag=LazyDeserializedDAG.from_dag(example_bash_op_dag),
            bundle_name="testing",
        )
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
        create_scheduler_dag(dag=dag).create_dagrun(
            run_id="test1",
            run_after=pendulum.datetime(2025, 1, 1, tz="UTC"),
            state=DagRunState.QUEUED,
            triggered_by=DagRunTriggeredByType.TEST,
            run_type=DagRunType.MANUAL,
        )

        dag.doc_md = "new doc string"
        SDM.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name="testing")
        serialized_dags2 = SDM.read_all_dags()
        sdags = session.query(SDM).all()
        # assert only the latest SDM is returned
        assert len(sdags) != len(serialized_dags2)

    def test_order_of_dag_params_is_stable(self):
        """
        This asserts that we have logic in place which guarantees the order
        of the params is maintained - even if the backend (e.g. MySQL) mutates
        the serialized DAG JSON.
        """
        example_dags = make_example_dags(example_dags_module)
        example_params_trigger_ui = example_dags.get("example_params_trigger_ui")
        before = list(example_params_trigger_ui.params.keys())

        SDM.write_dag(LazyDeserializedDAG.from_dag(example_params_trigger_ui), bundle_name="testing")
        retrieved_dag = SDM.get_dag("example_params_trigger_ui")
        after = list(retrieved_dag.params.keys())

        assert before == after

    @pytest.mark.db_test
    def test_order_of_deps_is_consistent(self, session):
        """
        Previously the 'dag_dependencies' node in serialized dag was converted to list from set.
        This caused the order, and thus the hash value, to be unreliable, which could produce
        excessive dag parsing.
        """

        db.clear_db_assets()
        session.add_all([AssetModel(id=i, uri=f"test://asset{i}/", name=f"{i}") for i in range(1, 6)])
        session.add_all([AssetModel(id=i, uri=f"test://asset{i}/", name=f"{i}*") for i in (0, 6)])
        session.commit()

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
            deps_order = [x["label"] for x in DagSerialization.serialize_dag(dag6)["dag_dependencies"]]
            # in below assert, 0 and 6 both come at end because "source" is different for them and source
            # is the first field in DagDependency class
            assert deps_order == ["1", "2", "3", "4", "5", "0*", "6*"]

            # for good measure, let's check that the dag hash is consistent
            dag_json = json.dumps(DagSerialization.to_dict(dag6), sort_keys=True).encode("utf-8")
            this_dag_hash = md5(dag_json).hexdigest()

            # set first dag hash on first pass
            if first_dag_hash is None:
                first_dag_hash = this_dag_hash

            # dag hash should not change without change in structure (we're in a loop)
            assert this_dag_hash == first_dag_hash
        db.clear_db_assets()

    def test_example_dag_hashes_are_always_consistent(self, session):
        """
        This test asserts that the hashes of the example dags are always consistent.
        """

        def get_hash_set():
            example_dags = self._write_example_dags()
            ordered_example_dags = dict(sorted(example_dags.items()))
            hashes = set()
            dag_hash_map = {}
            for dag_id in ordered_example_dags.keys():
                smd = session.execute(select(SDM.dag_hash).where(SDM.dag_id == dag_id)).one()
                hashes.add(smd.dag_hash)
                dag_hash_map[dag_id] = smd.dag_hash
            # TODO: Remove this logging once the origin of flaky test is identified and fixed.
            # Log (dag_id, hash) pairs for debugging flaky test failures.
            for dag_id, dag_hash in sorted(dag_hash_map.items()):
                logger.info("(%s, %s)", dag_id, dag_hash)
            return hashes

        first_hashes = get_hash_set()
        # assert that the hashes are the same
        assert first_hashes == get_hash_set()

    def test_get_latest_serdag_versions(self, dag_maker, session):
        # first dag
        with dag_maker("dag1") as dag:
            EmptyOperator(task_id="task1")
        sync_dag_to_db(dag, session=session)
        dag_maker.create_dagrun()
        with dag_maker("dag1") as dag:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        sync_dag_to_db(dag, session=session)
        dag_maker.create_dagrun(run_id="test2", logical_date=pendulum.datetime(2025, 1, 1))
        # second dag
        with dag_maker("dag2") as dag:
            EmptyOperator(task_id="task1")
        sync_dag_to_db(dag, session=session)
        dag_maker.create_dagrun(run_id="test3", logical_date=pendulum.datetime(2025, 1, 2))
        with dag_maker("dag2") as dag:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        sync_dag_to_db(dag, session=session)

        # Total serdags should be 4
        assert session.scalar(select(func.count()).select_from(SDM)) == 4

        latest_versions = SDM.get_latest_serialized_dags(dag_ids=["dag1", "dag2"], session=session)
        assert len(latest_versions) == 2

    def test_new_dag_versions_are_not_created_if_no_dagruns(self, dag_maker, session):
        with dag_maker("dag1") as dag:
            PythonOperator(task_id="task1", python_callable=lambda: None)
        assert session.query(SDM).count() == 1
        sdm1 = SDM.get(dag.dag_id, session=session)
        dag_hash = sdm1.dag_hash
        created_at = sdm1.created_at
        last_updated = sdm1.last_updated
        # new task
        PythonOperator(task_id="task2", python_callable=lambda: None, dag=dag)
        SDM.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name="dag_maker")
        sdm2 = SDM.get(dag.dag_id, session=session)

        assert sdm2.dag_hash != dag_hash  # first recorded serdag
        assert sdm2.created_at == created_at
        assert sdm2.last_updated != last_updated
        assert session.query(DagVersion).count() == 1
        assert session.query(SDM).count() == 1

    def test_new_dag_versions_are_created_if_there_is_a_dagrun(self, dag_maker, session):
        with dag_maker("dag1") as dag:
            PythonOperator(task_id="task1", python_callable=lambda: None)
        dag_maker.create_dagrun(run_id="test3", logical_date=pendulum.datetime(2025, 1, 2))
        assert session.query(SDM).count() == 1
        assert session.query(DagVersion).count() == 1
        # new task
        PythonOperator(task_id="task2", python_callable=lambda: None, dag=dag)
        SDM.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name="dag_maker")

        assert session.query(DagVersion).count() == 2
        assert session.query(SDM).count() == 2

    def test_example_dag_sorting_serialised_dag(self, session):
        """
        This test asserts if different dag ids -- simple or complex, can be sorted
        """
        example_dags = self._write_example_dags()

        for _, dag in example_dags.items():
            # flip the tags, the sorting function should sort it alphabetically
            if dag.tags:
                dag.tags = sorted(dag.tags, reverse=True)
            sorted_dag = SDM._sort_serialized_dag_dict(dag)
            assert sorted_dag == dag

    def test_get_dependencies(self, session):
        self._write_example_dags()
        dag_id = "consumes_asset_decorator"

        dependencies = SDM.get_dag_dependencies(session=session)
        assert dag_id in dependencies

        # Simulate deleting the DAG from file.
        session.execute(update(DagModel).where(DagModel.dag_id == dag_id).values(is_stale=True))
        dependencies = SDM.get_dag_dependencies(session=session)
        assert dag_id not in dependencies

    def test_get_dependencies_with_asset_ref(self, dag_maker, session):
        asset_name = "name"
        asset_uri = "test://asset1"
        asset_id = 1

        db.clear_db_assets()
        session.add_all(
            [
                AssetModel(id=asset_id, uri=asset_uri, name=asset_name),
                AssetActive(uri=asset_uri, name=asset_name),
            ]
        )
        session.commit()
        with dag_maker(
            dag_id="test_get_dependencies_with_asset_ref_example",
            start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
            schedule=[Asset.ref(uri=asset_uri), Asset.ref(uri="test://no-such-asset/")],
        ) as dag:
            BashOperator(task_id="any", bash_command="sleep 5")
        sync_dag_to_db(dag, session=session)

        dependencies = SDM.get_dag_dependencies(session=session)
        assert dependencies == {
            "test_get_dependencies_with_asset_ref_example": [
                DagDependency(
                    source="asset",
                    target="test_get_dependencies_with_asset_ref_example",
                    label=asset_name,
                    dependency_type="asset",
                    dependency_id=f"{asset_id}",
                ),
                DagDependency(
                    source="asset-uri-ref",
                    target="test_get_dependencies_with_asset_ref_example",
                    label="test://no-such-asset/",
                    dependency_type="asset-uri-ref",
                    dependency_id="test://no-such-asset/",
                ),
            ]
        }

        db.clear_db_assets()

    def test_get_dependencies_with_asset_alias(self, dag_maker, session):
        db.clear_db_assets()

        asset_name = "name"
        asset_uri = "test://asset1"
        asset_id = 1

        asset_model = AssetModel(id=asset_id, uri=asset_uri, name=asset_name)
        aam1 = AssetAliasModel(name="alias_1")  # resolve to asset
        aam2 = AssetAliasModel(name="alias_2")  # resolve to nothing

        session.add_all([aam1, aam2, asset_model, AssetActive.for_asset(asset_model)])
        aam1.assets.append(asset_model)
        session.commit()

        with dag_maker(
            dag_id="test_get_dependencies_with_asset_alias",
            start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
            schedule=[AssetAlias(name="alias_1"), AssetAlias(name="alias_2")],
        ) as dag:
            BashOperator(task_id="any", bash_command="sleep 5")
        sync_dag_to_db(dag, session=session)

        dependencies = SDM.get_dag_dependencies(session=session)
        assert dependencies == {
            "test_get_dependencies_with_asset_alias": [
                DagDependency(
                    source="asset",
                    target="asset-alias:alias_1",
                    label="name",
                    dependency_type="asset",
                    dependency_id="1",
                ),
                DagDependency(
                    source="asset:1",
                    target="test_get_dependencies_with_asset_alias",
                    label="alias_1",
                    dependency_type="asset-alias",
                    dependency_id="alias_1",
                ),
                DagDependency(
                    source="asset-alias",
                    target="test_get_dependencies_with_asset_alias",
                    label="alias_2",
                    dependency_type="asset-alias",
                    dependency_id="alias_2",
                ),
            ]
        }

        db.clear_db_assets()

    @pytest.mark.parametrize(
        ("provide_interval", "new_task", "should_write"),
        [
            (True, True, False),
            (True, False, False),
            (False, True, True),
            (False, False, False),
        ],
    )
    def test_min_update_interval_is_respected(self, provide_interval, new_task, should_write, dag_maker):
        min_update_interval = 10 if provide_interval else 0
        with dag_maker("dag1") as dag:
            PythonOperator(task_id="task1", python_callable=lambda: None)

        if new_task:
            PythonOperator(task_id="task2", python_callable=lambda: None, dag=dag)

        did_write = SDM.write_dag(
            LazyDeserializedDAG.from_dag(dag),
            bundle_name="dag_maker",
            min_update_interval=min_update_interval,
        )
        assert did_write is should_write

    def test_new_dag_version_created_when_bundle_name_changes_and_hash_unchanged(self, dag_maker, session):
        """Test that new dag_version is created if bundle_name changes but DAG is unchanged."""
        # Create and write initial DAG
        initial_bundle = "bundleA"
        with dag_maker("test_dag_update_bundle", bundle_name=initial_bundle) as dag:
            EmptyOperator(task_id="task1")

        # Create TIs
        dag_maker.create_dagrun(run_id="test_run")

        assert session.query(DagVersion).count() == 1

        # Write the same DAG (no changes, so hash is the same) with a new bundle_name
        new_bundle = "bundleB"
        SDM.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name=new_bundle)

        # There should now be two versions of the DAG
        assert session.query(DagVersion).count() == 2

    def test_hash_method_removes_fileloc_and_remains_consistent(self):
        """Test that the hash method removes fileloc before hashing."""
        test_data = {
            "__version": 1,
            "dag": {
                "fileloc": "/path/to/dag.py",
                "dag_id": "test_dag",
                "tasks": {
                    "task1": {"task_id": "task1"},
                },
            },
        }

        hash_with_fileloc = SDM.hash(test_data)

        # Modify only the top-level dag.fileloc path (simulating file location changes)
        test_data["dag"]["fileloc"] = "/different/path/to/dag.py"

        # Get hash with different top-level fileloc (should be the same)
        hash_with_different_fileloc = SDM.hash(test_data)

        # Hashes should be identical since top-level dag.fileloc is removed before hashing
        assert hash_with_fileloc == hash_with_different_fileloc

        # Verify that the original data still has fileloc (method shouldn't modify original)
        assert "fileloc" in test_data["dag"]
        assert test_data["dag"]["fileloc"] == "/different/path/to/dag.py"

    def test_hash_method_consistent_with_dict_ordering_in_template_fields(self, dag_maker):
        from airflow.sdk.bases.operator import BaseOperator

        class MyCustomOp(BaseOperator):
            template_fields = ("env_vars",)

            def __init__(self, *, task_id: str, **kwargs):
                super().__init__(task_id=task_id, **kwargs)
                self.env_vars = {"KEY1": "value1", "KEY2": "value2", "KEY3": "value3"}

        # Create first DAG with env_vars in one order
        with dag_maker("test_dag") as dag1:
            MyCustomOp(task_id="task1")

        serialized_dag_1 = DagSerialization.to_dict(dag1)

        # Create second DAG with env_vars in different order
        with dag_maker("test_dag") as dag2:
            task = MyCustomOp(task_id="task1")
            # Recreate dict with different insertion order
            task.env_vars = {"KEY3": "value3", "KEY1": "value1", "KEY2": "value2"}

        serialized_dag_2 = DagSerialization.to_dict(dag2)

        # Verify that the original env_vars have different ordering
        env_vars_1 = None
        env_vars_2 = None
        for task in serialized_dag_1["dag"]["tasks"]:
            if task["__var"]["task_id"] == "task1":
                env_vars_1 = task["__var"].get("env_vars")
        for task in serialized_dag_2["dag"]["tasks"]:
            if task["__var"]["task_id"] == "task1":
                env_vars_2 = task["__var"].get("env_vars")

        assert env_vars_1 is not None, "serialized_dag_1 should have env_vars"
        assert env_vars_2 is not None, "serialized_dag_2 should have env_vars"
        # The serialized env_vars should be sorted dicts (or strings if truncated)
        # If they're dicts, verify they're sorted; if strings, they should be equal due to sorting
        if isinstance(env_vars_1, dict) and isinstance(env_vars_2, dict):
            # Both should be sorted dictionaries with same content
            assert list(env_vars_1.keys()) == sorted(env_vars_1.keys())
            assert list(env_vars_2.keys()) == sorted(env_vars_2.keys())
            assert env_vars_1 == env_vars_2, "Sorted dicts should be equal regardless of original order"
        elif isinstance(env_vars_1, str) and isinstance(env_vars_2, str):
            # If truncated to strings, they should be equal due to sorting
            assert env_vars_1 == env_vars_2, "String representations should be equal due to sorting"

        hash_1 = SDM.hash(serialized_dag_1)
        hash_2 = SDM.hash(serialized_dag_2)

        # Hashes should be identical
        assert hash_1 == hash_2, "Hashes should be identical when dicts are sorted consistently"

    def test_dynamic_dag_update_preserves_null_check(self, dag_maker, session):
        """
        Test that dynamic DAG update gracefully handles case where SerializedDagModel doesn't exist.
        This preserves the null-check fix from PR #56422 and tests the direct UPDATE path.
        """
        with dag_maker(dag_id="test_missing_serdag", serialized=True, session=session) as dag:
            EmptyOperator(task_id="task1")

        # Write the DAG first
        lazy_dag = LazyDeserializedDAG.from_dag(dag)
        SDM.write_dag(
            dag=lazy_dag,
            bundle_name="test_bundle",
            bundle_version=None,
            session=session,
        )
        session.commit()

        # Get the dag_version
        dag_version = session.scalar(
            select(DagVersion).where(DagVersion.dag_id == "test_missing_serdag").limit(1)
        )
        assert dag_version is not None

        # Manually delete SerializedDagModel (simulates edge case)
        session.query(SDM).filter(SDM.dag_id == "test_missing_serdag").delete()
        session.commit()

        # Verify no SerializedDagModel exists
        assert SDM.get("test_missing_serdag", session=session) is None

        # Try to update - should return False gracefully (not crash)
        result = SDM.write_dag(
            dag=lazy_dag,
            bundle_name="test_bundle",
            bundle_version=None,
            min_update_interval=None,
            session=session,
        )

        assert result is False  # Should return False when SerializedDagModel is missing

    def test_dynamic_dag_update_success(self, dag_maker, session):
        """
        Test that dynamic DAG update successfully updates the serialized DAG hash
        when no task instances exist.
        """
        with dag_maker(dag_id="test_dynamic_success", session=session) as dag:
            EmptyOperator(task_id="task1")

        # Write the DAG first
        lazy_dag = LazyDeserializedDAG.from_dag(dag)
        result1 = SDM.write_dag(
            dag=lazy_dag,
            bundle_name="test_bundle",
            bundle_version=None,
            session=session,
        )
        session.commit()

        assert result1 is True
        initial_sdag = SDM.get("test_dynamic_success", session=session)
        assert initial_sdag is not None
        initial_hash = initial_sdag.dag_hash

        # Modify the DAG (add a task)
        EmptyOperator(task_id="task2", dag=dag)
        lazy_dag_updated = LazyDeserializedDAG.from_dag(dag)

        # Write again - should use UPDATE path (no task instances yet)
        result2 = SDM.write_dag(
            dag=lazy_dag_updated,
            bundle_name="test_bundle",
            bundle_version=None,
            session=session,
        )
        session.commit()

        # Verify update succeeded
        assert result2 is True
        updated_sdag = SDM.get("test_dynamic_success", session=session)
        assert updated_sdag.dag_hash != initial_hash  # Hash should change
        assert len(updated_sdag.dag.task_dict) == 2  # Should have 2 tasks now

    def test_write_dag_atomicity_on_dagcode_failure(self, dag_maker, session):
        """
        Test that SerializedDagModel.write_dag maintains atomicity.

        If DagCode.write_code fails, the entire transaction should rollback,
        including the DagVersion. This test verifies that DagVersion is not
        committed separately, which would leave orphaned records.

        This test would fail if DagVersion.write_dag() was used (which commits
        immediately), because the DagVersion would be persisted even though
        the rest of the transaction failed.
        """
        from airflow.models.dagcode import DagCode

        with dag_maker("test_atomicity_dag"):
            EmptyOperator(task_id="task1")

        dag = dag_maker.dag
        initial_version_count = session.query(DagVersion).filter(DagVersion.dag_id == dag.dag_id).count()
        assert initial_version_count == 1, "Should have one DagVersion after initial write"
        dag_maker.create_dagrun()  # ensure the second dag version is created

        EmptyOperator(task_id="task2", dag=dag)
        modified_lazy_dag = LazyDeserializedDAG.from_dag(dag)

        # Mock DagCode.write_code to raise an exception
        with mock.patch.object(
            DagCode, "write_code", side_effect=RuntimeError("Simulated DagCode.write_code failure")
        ):
            with pytest.raises(RuntimeError, match="Simulated DagCode.write_code failure"):
                SDM.write_dag(
                    dag=modified_lazy_dag,
                    bundle_name="testing",
                    bundle_version=None,
                    session=session,
                )
            session.rollback()

            # Verify that no new DagVersion was committed
            # Use a fresh session to ensure we're reading from committed data
            with create_session() as fresh_session:
                final_version_count = (
                    fresh_session.query(DagVersion).filter(DagVersion.dag_id == dag.dag_id).count()
                )
                assert final_version_count == initial_version_count, (
                    "DagVersion should not be committed when DagCode.write_code fails"
                )

                sdag = SDM.get(dag.dag_id, session=fresh_session)
                assert sdag is not None, "Original SerializedDagModel should still exist"
                assert len(sdag.dag.task_dict) == 1, (
                    "SerializedDagModel should not be updated when write fails"
                )
