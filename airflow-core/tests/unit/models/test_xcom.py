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

import datetime
import operator
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.models.dag import DAG
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time.xcom import resolve_xcom_backend
from airflow.settings import json

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs, clear_db_xcom
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

pytestmark = pytest.mark.db_test


if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class CustomXCom(BaseXCom): ...


@pytest.fixture(autouse=True)
def reset_db():
    """Reset XCom entries."""
    clear_db_dags()
    clear_db_runs()
    clear_db_xcom()
    clear_db_dag_bundles()


@pytest.fixture
def task_instance_factory(request, session: Session):
    def func(*, dag_id, task_id, logical_date, run_after=None):
        sync_dag_to_db(DAG(dag_id=dag_id))
        run_id = DagRun.generate_run_id(
            run_type=DagRunType.SCHEDULED,
            logical_date=logical_date,
            run_after=run_after if run_after is not None else logical_date,
        )
        interval = (logical_date, logical_date) if logical_date else None
        run = DagRun(
            dag_id=dag_id,
            run_type=DagRunType.SCHEDULED,
            run_id=run_id,
            logical_date=logical_date,
            data_interval=interval,
            run_after=run_after if run_after is not None else logical_date,
        )
        session.add(run)
        session.flush()
        dag_version = DagVersion.get_latest_version(run.dag_id, session=session)
        ti = TaskInstance(EmptyOperator(task_id=task_id), run_id=run_id, dag_version_id=dag_version.id)
        ti.dag_id = dag_id
        session.add(ti)
        session.commit()

        def cleanup_database():
            # This should also clear task instances by cascading.
            session.query(DagRun).filter_by(id=run.id).delete()
            session.commit()

        request.addfinalizer(cleanup_database)
        return ti

    return func


@pytest.fixture
def task_instance(task_instance_factory):
    return task_instance_factory(
        dag_id="dag",
        task_id="task_1",
        logical_date=timezone.datetime(2021, 12, 3, 4, 56),
    )


@pytest.fixture
def task_instances(session, task_instance):
    ti2 = TaskInstance(
        EmptyOperator(task_id="task_2"),
        run_id=task_instance.run_id,
        dag_version_id=task_instance.dag_version_id,
    )
    ti2.dag_id = task_instance.dag_id
    session.add(ti2)
    session.commit()
    return task_instance, ti2  # ti2 will be cleaned up automatically with the DAG run.


class TestXCom:
    @conf_vars({("core", "xcom_backend"): "unit.models.test_xcom.CustomXCom"})
    def test_resolve_xcom_class(self):
        cls = resolve_xcom_backend()
        assert issubclass(cls, CustomXCom)

    @conf_vars({("core", "xcom_backend"): ""})
    def test_resolve_xcom_class_fallback_to_basexcom(self):
        cls = resolve_xcom_backend()
        assert issubclass(cls, BaseXCom)
        assert cls.serialize_value([1]) == [1]

    @conf_vars({("core", "xcom_backend"): "to be removed"})
    def test_resolve_xcom_class_fallback_to_basexcom_no_config(self):
        from airflow.sdk.configuration import conf as sdk_conf

        conf.remove_option("core", "xcom_backend")
        sdk_conf.remove_option("core", "xcom_backend")
        cls = resolve_xcom_backend()
        assert issubclass(cls, BaseXCom)
        assert cls.serialize_value([1]) == [1]

    @skip_if_force_lowest_dependencies_marker
    @mock.patch("airflow.sdk.execution_time.xcom.conf.getimport")
    def test_set_serialize_call_current_signature(self, get_import, task_instance, mock_supervisor_comms):
        """
        When XCom.serialize_value includes params logical_date, key, dag_id, task_id and run_id,
        then XCom.set should pass all of them.
        """
        serialize_watcher = MagicMock()

        class CurrentSignatureXCom(BaseXCom):
            @staticmethod
            def serialize_value(
                value,
                key=None,
                dag_id=None,
                task_id=None,
                run_id=None,
                map_index=None,
            ):
                serialize_watcher(
                    value=value,
                    key=key,
                    dag_id=dag_id,
                    task_id=task_id,
                    run_id=run_id,
                    map_index=map_index,
                )
                return json.dumps(value)

        get_import.return_value = CurrentSignatureXCom

        XCom = resolve_xcom_backend()
        XCom.set(
            key=XCom.XCOM_RETURN_KEY,
            value={"my_xcom_key": "my_xcom_value"},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            map_index=-1,
        )
        serialize_watcher.assert_called_once_with(
            key=XCom.XCOM_RETURN_KEY,
            value={"my_xcom_key": "my_xcom_value"},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            map_index=-1,
        )


@pytest.fixture
def push_simple_json_xcom(session):
    def func(*, ti: TaskInstance, key: str, value):
        return XComModel.set(
            key=key,
            value=value,
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            session=session,
        )

    return func


class TestXComGet:
    @pytest.fixture
    def setup_for_xcom_get_one(self, task_instance, push_simple_json_xcom):
        push_simple_json_xcom(ti=task_instance, key="xcom_1", value={"key": "value"})

    @pytest.mark.usefixtures("setup_for_xcom_get_one")
    def test_xcom_get_one(self, session, task_instance):
        stored_value = session.execute(
            XComModel.get_many(
                key="xcom_1",
                dag_ids=task_instance.dag_id,
                task_ids=task_instance.task_id,
                run_id=task_instance.run_id,
            ).with_only_columns(XComModel.value)
        ).first()
        assert XComModel.deserialize_value(stored_value) == {"key": "value"}

    @pytest.fixture
    def tis_for_xcom_get_one_from_prior_date(self, task_instance_factory, push_simple_json_xcom):
        date1 = timezone.datetime(2021, 12, 3, 4, 56)
        ti1 = task_instance_factory(dag_id="dag", logical_date=date1, task_id="task_1")
        ti2 = task_instance_factory(
            dag_id="dag",
            logical_date=date1 + datetime.timedelta(days=1),
            task_id="task_1",
        )

        # The earlier run pushes an XCom, but not the later run, but the later
        # run can get this earlier XCom with ``include_prior_dates``.
        push_simple_json_xcom(ti=ti1, key="xcom_1", value={"key": "value"})

        return ti1, ti2

    @pytest.fixture
    def tis_for_xcom_get_one_from_prior_date_without_logical_date(
        self, task_instance_factory, push_simple_json_xcom
    ):
        date1 = timezone.datetime(2021, 12, 3, 4, 56)
        ti1 = task_instance_factory(dag_id="dag", logical_date=None, task_id="task_1", run_after=date1)
        ti2 = task_instance_factory(
            dag_id="dag",
            logical_date=None,
            run_after=date1 + datetime.timedelta(days=1),
            task_id="task_1",
        )

        # The earlier run pushes an XCom, but not the later run, but the later
        # run can get this earlier XCom with ``include_prior_dates``.
        push_simple_json_xcom(ti=ti1, key="xcom_1", value={"key": "value"})

        return ti1, ti2

    def test_xcom_get_one_from_prior_date(self, session, tis_for_xcom_get_one_from_prior_date):
        _, ti2 = tis_for_xcom_get_one_from_prior_date
        retrieved_value = session.execute(
            XComModel.get_many(
                run_id=ti2.run_id,
                key="xcom_1",
                task_ids="task_1",
                dag_ids="dag",
                include_prior_dates=True,
            ).with_only_columns(XComModel.value)
        ).first()
        assert XComModel.deserialize_value(retrieved_value) == {"key": "value"}

    def test_xcom_get_one_from_prior_date_with_no_logical_dates(
        self, session, tis_for_xcom_get_one_from_prior_date_without_logical_date
    ):
        _, ti2 = tis_for_xcom_get_one_from_prior_date_without_logical_date
        retrieved_value = session.execute(
            XComModel.get_many(
                run_id=ti2.run_id,
                key="xcom_1",
                task_ids="task_1",
                dag_ids="dag",
                include_prior_dates=True,
            ).with_only_columns(XComModel.value)
        ).first()
        assert XComModel.deserialize_value(retrieved_value) == {"key": "value"}

    @pytest.fixture
    def setup_for_xcom_get_many_single_argument_value(self, task_instance, push_simple_json_xcom):
        push_simple_json_xcom(ti=task_instance, key="xcom_1", value={"key": "value"})

    @pytest.mark.usefixtures("setup_for_xcom_get_many_single_argument_value")
    def test_xcom_get_many_single_argument_value(self, session, task_instance):
        stored_xcoms = session.scalars(
            XComModel.get_many(
                key="xcom_1",
                dag_ids=task_instance.dag_id,
                task_ids=task_instance.task_id,
                run_id=task_instance.run_id,
            )
        ).all()
        assert len(stored_xcoms) == 1
        assert stored_xcoms[0].key == "xcom_1"
        assert stored_xcoms[0].value == json.dumps({"key": "value"})

    @pytest.fixture
    def setup_for_xcom_get_many_multiple_tasks(self, task_instances, push_simple_json_xcom):
        ti1, ti2 = task_instances
        push_simple_json_xcom(ti=ti1, key="xcom_1", value={"key1": "value1"})
        push_simple_json_xcom(ti=ti2, key="xcom_1", value={"key2": "value2"})

    @pytest.mark.usefixtures("setup_for_xcom_get_many_multiple_tasks")
    def test_xcom_get_many_multiple_tasks(self, session, task_instance):
        stored_xcoms = session.scalars(
            XComModel.get_many(
                key="xcom_1",
                dag_ids=task_instance.dag_id,
                task_ids=["task_1", "task_2"],
                run_id=task_instance.run_id,
            )
        ).all()
        sorted_values = [x.value for x in sorted(stored_xcoms, key=operator.attrgetter("task_id"))]
        assert sorted_values == [json.dumps({"key1": "value1"}), json.dumps({"key2": "value2"})]

    @pytest.fixture
    def tis_for_xcom_get_many_from_prior_dates(self, task_instance_factory, push_simple_json_xcom):
        date1 = timezone.datetime(2021, 12, 3, 4, 56)
        date2 = date1 + datetime.timedelta(days=1)
        ti1 = task_instance_factory(dag_id="dag", task_id="task_1", logical_date=date1)
        ti2 = task_instance_factory(dag_id="dag", task_id="task_1", logical_date=date2)
        push_simple_json_xcom(ti=ti1, key="xcom_1", value={"key1": "value1"})
        push_simple_json_xcom(ti=ti2, key="xcom_1", value={"key2": "value2"})
        return ti1, ti2

    def test_xcom_get_many_from_prior_dates(self, session, tis_for_xcom_get_many_from_prior_dates):
        ti1, ti2 = tis_for_xcom_get_many_from_prior_dates
        session.add(ti1)  # for some reason, ti1 goes out of the session scope
        stored_xcoms = session.scalars(
            XComModel.get_many(
                run_id=ti2.run_id,
                key="xcom_1",
                dag_ids="dag",
                task_ids="task_1",
                include_prior_dates=True,
            )
        ).all()

        # The retrieved XComs should be ordered by logical date, latest first.
        assert [x.value for x in stored_xcoms] == list(
            map(lambda j: json.dumps(j), [{"key2": "value2"}, {"key1": "value1"}])
        )
        assert [x.logical_date for x in stored_xcoms] == [ti2.logical_date, ti1.logical_date]

    def test_xcom_get_invalid_key(self, session, task_instance):
        """Test that getting an XCom with an invalid key raises a ValueError."""
        with pytest.raises(ValueError, match="XCom key must be a non-empty string. Received: ''"):
            XComModel.get_many(
                key="",  # Invalid key
                dag_ids=task_instance.dag_id,
                task_ids=task_instance.task_id,
                run_id=task_instance.run_id,
            )


class TestXComSet:
    @pytest.mark.parametrize(
        ("key", "value", "expected_value"),
        [
            pytest.param("xcom_dict", {"key": "value"}, {"key": "value"}, id="dict"),
            pytest.param("xcom_int", 123, 123, id="int"),
            pytest.param("xcom_float", 45.67, 45.67, id="float"),
            pytest.param("xcom_str", "hello", "hello", id="str"),
            pytest.param("xcom_bool", True, True, id="bool"),
            pytest.param("xcom_list", [1, 2, 3], [1, 2, 3], id="list"),
        ],
    )
    def test_xcom_set(self, session, task_instance, key, value, expected_value):
        XComModel.set(
            key=key,
            value=value,
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )
        stored_xcoms = session.query(XComModel).all()
        assert stored_xcoms[0].key == key
        assert isinstance(stored_xcoms[0].value, type(json.dumps(expected_value)))
        assert stored_xcoms[0].value == json.dumps(expected_value)
        assert stored_xcoms[0].dag_id == "dag"
        assert stored_xcoms[0].task_id == "task_1"
        assert stored_xcoms[0].logical_date == task_instance.logical_date

    @pytest.fixture
    def setup_for_xcom_set_again_replace(self, task_instance, push_simple_json_xcom):
        push_simple_json_xcom(ti=task_instance, key="xcom_1", value={"key1": "value1"})

    @pytest.mark.usefixtures("setup_for_xcom_set_again_replace")
    def test_xcom_set_again_replace(self, session, task_instance):
        assert session.query(XComModel).one().value == json.dumps({"key1": "value1"})
        XComModel.set(
            key="xcom_1",
            value={"key2": "value2"},
            dag_id=task_instance.dag_id,
            task_id="task_1",
            run_id=task_instance.run_id,
            session=session,
        )
        assert session.query(XComModel).one().value == json.dumps({"key2": "value2"})

    def test_xcom_set_invalid_key(self, session, task_instance):
        """Test that setting an XCom with an invalid key raises a ValueError."""
        with pytest.raises(ValueError, match="XCom key must be a non-empty string. Received: ''"):
            XComModel.set(
                key="",  # Invalid key
                value={"key": "value"},
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                run_id=task_instance.run_id,
                session=session,
            )

        with pytest.raises(ValueError, match="XCom key must be a non-empty string. Received: None"):
            XComModel.set(
                key=None,  # Invalid key
                value={"key": "value"},
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                run_id=task_instance.run_id,
                session=session,
            )


class TestXComClear:
    @pytest.fixture
    def setup_for_xcom_clear(self, task_instance, push_simple_json_xcom):
        push_simple_json_xcom(ti=task_instance, key="xcom_1", value={"key": "value"})

    @pytest.mark.usefixtures("setup_for_xcom_clear")
    @mock.patch("airflow.sdk.execution_time.xcom.XCom.purge")
    def test_xcom_clear(self, mock_purge, session, task_instance):
        assert session.query(XComModel).count() == 1
        XComModel.clear(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )
        assert session.query(XComModel).count() == 0
        # purge will not be done when we clear, will be handled in task sdk
        assert mock_purge.call_count == 0

    @pytest.mark.usefixtures("setup_for_xcom_clear")
    def test_xcom_clear_different_run(self, session, task_instance):
        XComModel.clear(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id="different_run",
            session=session,
        )
        assert session.query(XComModel).count() == 1


class TestXComRoundTrip:
    @pytest.mark.parametrize(
        ("value", "expected_value"),
        [
            pytest.param(1, 1, id="int"),
            pytest.param(1.0, 1.0, id="float"),
            pytest.param("string", "string", id="str"),
            pytest.param(True, True, id="bool"),
            pytest.param({"key": "value"}, {"key": "value"}, id="dict"),
            pytest.param([1, 2, 3], [1, 2, 3], id="list"),
            pytest.param((1, 2, 3), (1, 2, 3), id="tuple"),  # tuple is preserved
            pytest.param(None, None, id="none"),
        ],
    )
    def test_xcom_round_trip(self, value, expected_value, push_simple_json_xcom, task_instance, session):
        """Test that XComModel serialization and deserialization work as expected."""
        push_simple_json_xcom(ti=task_instance, key="xcom_1", value=value)

        stored_value = session.execute(
            XComModel.get_many(
                key="xcom_1",
                dag_ids=task_instance.dag_id,
                task_ids=task_instance.task_id,
                run_id=task_instance.run_id,
            ).with_only_columns(XComModel.value)
        ).first()
        deserialized_value = XComModel.deserialize_value(stored_value)

        assert deserialized_value == expected_value

    @pytest.mark.parametrize(
        ("value", "expected_value"),
        [
            pytest.param(1, 1, id="int"),
            pytest.param(1.0, 1.0, id="float"),
            pytest.param("string", "string", id="str"),
            pytest.param(True, True, id="bool"),
            pytest.param({"key": "value"}, {"key": "value"}, id="dict"),
            pytest.param([1, 2, 3], [1, 2, 3], id="list"),
            pytest.param((1, 2, 3), (1, 2, 3), id="tuple"),  # tuple is preserved
            pytest.param(None, None, id="none"),
        ],
    )
    def test_xcom_deser_fallback(self, value, expected_value):
        """Test fallback in deserialization."""

        mock_xcom = MagicMock(value=value)
        deserialized_value = XComModel.deserialize_value(mock_xcom)

        assert deserialized_value == expected_value
