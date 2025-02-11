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

from airflow.configuration import conf
from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import BaseXCom, XCom, resolve_xcom_backend
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.settings import json
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.xcom import XCOM_RETURN_KEY

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class CustomXCom(BaseXCom):
    orm_deserialize_value = mock.Mock()


@pytest.fixture(autouse=True)
def reset_db():
    """Reset XCom entries."""
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(XCom).delete()


@pytest.fixture
def task_instance_factory(request, session: Session):
    def func(*, dag_id, task_id, logical_date):
        run_id = DagRun.generate_run_id(
            run_type=DagRunType.SCHEDULED, logical_date=logical_date, run_after=logical_date
        )
        run = DagRun(
            dag_id=dag_id,
            run_type=DagRunType.SCHEDULED,
            run_id=run_id,
            logical_date=logical_date,
            data_interval=(logical_date, logical_date),
            run_after=logical_date,
        )
        session.add(run)
        ti = TaskInstance(EmptyOperator(task_id=task_id), run_id=run_id)
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
    ti2 = TaskInstance(EmptyOperator(task_id="task_2"), run_id=task_instance.run_id)
    ti2.dag_id = task_instance.dag_id
    session.add(ti2)
    session.commit()
    return task_instance, ti2  # ti2 will be cleaned up automatically with the DAG run.


class TestXCom:
    @conf_vars({("core", "xcom_backend"): "tests.models.test_xcom.CustomXCom"})
    def test_resolve_xcom_class(self):
        cls = resolve_xcom_backend()
        assert issubclass(cls, CustomXCom)

    @conf_vars({("core", "xcom_backend"): ""})
    def test_resolve_xcom_class_fallback_to_basexcom(self):
        cls = resolve_xcom_backend()
        assert issubclass(cls, BaseXCom)
        assert cls.serialize_value([1]) == "[1]"

    @conf_vars({("core", "xcom_backend"): "to be removed"})
    def test_resolve_xcom_class_fallback_to_basexcom_no_config(self):
        conf.remove_option("core", "xcom_backend")
        cls = resolve_xcom_backend()
        assert issubclass(cls, BaseXCom)
        assert cls.serialize_value([1]) == "[1]"

    @mock.patch("airflow.models.xcom.XCom.orm_deserialize_value")
    def test_xcom_init_on_load_uses_orm_deserialize_value(self, mock_orm_deserialize):
        instance = BaseXCom(
            key="key",
            value="value",
            timestamp=timezone.utcnow(),
            logical_date=timezone.utcnow(),
            task_id="task_id",
            dag_id="dag_id",
        )
        instance.init_on_load()
        mock_orm_deserialize.assert_called_once_with()

    @conf_vars({("core", "xcom_backend"): "tests.models.test_xcom.CustomXCom"})
    def test_get_one_custom_backend_no_use_orm_deserialize_value(self, task_instance, session):
        """Test that XCom.get_one does not call orm_deserialize_value"""
        XCom = resolve_xcom_backend()
        XCom.set(
            key=XCOM_RETURN_KEY,
            value={"key": "value"},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )

        value = XCom.get_one(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )
        assert value == {"key": "value"}
        XCom.orm_deserialize_value.assert_not_called()

    @mock.patch("airflow.models.xcom.conf.getimport")
    def test_set_serialize_call_current_signature(self, get_import, task_instance):
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
            key=XCOM_RETURN_KEY,
            value={"my_xcom_key": "my_xcom_value"},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            map_index=-1,
        )
        serialize_watcher.assert_called_once_with(
            key=XCOM_RETURN_KEY,
            value={"my_xcom_key": "my_xcom_value"},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            map_index=-1,
        )


@pytest.fixture
def push_simple_json_xcom(session):
    def func(*, ti: TaskInstance, key: str, value):
        return XCom.set(
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
        stored_value = XCom.get_one(
            key="xcom_1",
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )
        assert stored_value == {"key": "value"}

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

    def test_xcom_get_one_from_prior_date(self, session, tis_for_xcom_get_one_from_prior_date):
        _, ti2 = tis_for_xcom_get_one_from_prior_date
        retrieved_value = XCom.get_one(
            run_id=ti2.run_id,
            key="xcom_1",
            task_id="task_1",
            dag_id="dag",
            include_prior_dates=True,
            session=session,
        )
        assert retrieved_value == {"key": "value"}

    @pytest.fixture
    def setup_for_xcom_get_many_single_argument_value(self, task_instance, push_simple_json_xcom):
        push_simple_json_xcom(ti=task_instance, key="xcom_1", value={"key": "value"})

    @pytest.mark.usefixtures("setup_for_xcom_get_many_single_argument_value")
    def test_xcom_get_many_single_argument_value(self, session, task_instance):
        stored_xcoms = XCom.get_many(
            key="xcom_1",
            dag_ids=task_instance.dag_id,
            task_ids=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        ).all()
        assert len(stored_xcoms) == 1
        assert stored_xcoms[0].key == "xcom_1"
        assert stored_xcoms[0].value == {"key": "value"}

    @pytest.fixture
    def setup_for_xcom_get_many_multiple_tasks(self, task_instances, push_simple_json_xcom):
        ti1, ti2 = task_instances
        push_simple_json_xcom(ti=ti1, key="xcom_1", value={"key1": "value1"})
        push_simple_json_xcom(ti=ti2, key="xcom_1", value={"key2": "value2"})

    @pytest.mark.usefixtures("setup_for_xcom_get_many_multiple_tasks")
    def test_xcom_get_many_multiple_tasks(self, session, task_instance):
        stored_xcoms = XCom.get_many(
            key="xcom_1",
            dag_ids=task_instance.dag_id,
            task_ids=["task_1", "task_2"],
            run_id=task_instance.run_id,
            session=session,
        )
        sorted_values = [x.value for x in sorted(stored_xcoms, key=operator.attrgetter("task_id"))]
        assert sorted_values == [{"key1": "value1"}, {"key2": "value2"}]

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
        stored_xcoms = XCom.get_many(
            run_id=ti2.run_id,
            key="xcom_1",
            dag_ids="dag",
            task_ids="task_1",
            include_prior_dates=True,
            session=session,
        )

        # The retrieved XComs should be ordered by logical date, latest first.
        assert [x.value for x in stored_xcoms] == [{"key2": "value2"}, {"key1": "value1"}]
        assert [x.logical_date for x in stored_xcoms] == [ti2.logical_date, ti1.logical_date]


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
        XCom.set(
            key=key,
            value=value,
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )
        stored_xcoms = session.query(XCom).all()
        assert stored_xcoms[0].key == key
        assert isinstance(stored_xcoms[0].value, type(expected_value))
        assert stored_xcoms[0].value == expected_value
        assert stored_xcoms[0].dag_id == "dag"
        assert stored_xcoms[0].task_id == "task_1"
        assert stored_xcoms[0].logical_date == task_instance.logical_date

    @pytest.fixture
    def setup_for_xcom_set_again_replace(self, task_instance, push_simple_json_xcom):
        push_simple_json_xcom(ti=task_instance, key="xcom_1", value={"key1": "value1"})

    @pytest.mark.usefixtures("setup_for_xcom_set_again_replace")
    def test_xcom_set_again_replace(self, session, task_instance):
        assert session.query(XCom).one().value == {"key1": "value1"}
        XCom.set(
            key="xcom_1",
            value={"key2": "value2"},
            dag_id=task_instance.dag_id,
            task_id="task_1",
            run_id=task_instance.run_id,
            session=session,
        )
        assert session.query(XCom).one().value == {"key2": "value2"}


class TestXComClear:
    @pytest.fixture
    def setup_for_xcom_clear(self, task_instance, push_simple_json_xcom):
        push_simple_json_xcom(ti=task_instance, key="xcom_1", value={"key": "value"})

    @pytest.mark.usefixtures("setup_for_xcom_clear")
    @mock.patch("airflow.models.xcom.XCom.purge")
    def test_xcom_clear(self, mock_purge, session, task_instance):
        assert session.query(XCom).count() == 1
        XCom.clear(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )
        assert session.query(XCom).count() == 0
        assert mock_purge.call_count == 1

    @pytest.mark.usefixtures("setup_for_xcom_clear")
    def test_xcom_clear_different_run(self, session, task_instance):
        XCom.clear(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id="different_run",
            session=session,
        )
        assert session.query(XCom).count() == 1
