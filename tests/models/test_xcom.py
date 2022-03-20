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
import datetime
import operator
import os
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.configuration import conf
from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.taskinstance import TaskInstanceKey
from airflow.models.xcom import IN_MEMORY_RUN_ID, XCOM_RETURN_KEY, BaseXCom, XCom, resolve_xcom_backend
from airflow.settings import json
from airflow.utils import timezone
from airflow.utils.session import create_session
from tests.test_utils.config import conf_vars


class CustomXCom(BaseXCom):
    orm_deserialize_value = mock.Mock()


@pytest.fixture(autouse=True)
def reset_db():
    """Reset XCom entries."""
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(XCom).delete()


@pytest.fixture()
def dag_run_factory(request, session):
    def func(dag_id, execution_date):
        run = DagRun(
            dag_id=dag_id,
            run_type=DagRunType.SCHEDULED,
            run_id=DagRun.generate_run_id(DagRunType.SCHEDULED, execution_date),
            execution_date=execution_date,
        )
        session.add(run)
        session.commit()

        def delete_dagrun():
            session.query(DagRun).filter(DagRun.id == run.id).delete()
            session.commit()

        request.addfinalizer(delete_dagrun)
        return run

    yield func


@pytest.fixture()
def dag_run(dag_run_factory):
    return dag_run_factory(dag_id="dag", execution_date=timezone.datetime(2021, 12, 3, 4, 56))


class TestXCom:
    @conf_vars({("core", "xcom_backend"): "tests.models.test_xcom.CustomXCom"})
    def test_resolve_xcom_class(self):
        cls = resolve_xcom_backend()
        assert issubclass(cls, CustomXCom)

    @conf_vars({("core", "xcom_backend"): "", ("core", "enable_xcom_pickling"): "False"})
    def test_resolve_xcom_class_fallback_to_basexcom(self):
        cls = resolve_xcom_backend()
        assert issubclass(cls, BaseXCom)
        assert cls.serialize_value([1]) == b"[1]"

    @conf_vars({("core", "enable_xcom_pickling"): "False"})
    @conf_vars({("core", "xcom_backend"): "to be removed"})
    def test_resolve_xcom_class_fallback_to_basexcom_no_config(self):
        conf.remove_option("core", "xcom_backend")
        cls = resolve_xcom_backend()
        assert issubclass(cls, BaseXCom)
        assert cls.serialize_value([1]) == b"[1]"

    def test_xcom_deserialize_with_json_to_pickle_switch(self, dag_run, session):
        ti_key = TaskInstanceKey(
            dag_id=dag_run.dag_id,
            task_id="test_task3",
            run_id=dag_run.run_id,
        )
        with conf_vars({("core", "enable_xcom_pickling"): "False"}):
            XCom.set(
                key="xcom_test3",
                value={"key": "value"},
                dag_id=dag_run.dag_id,
                task_id="test_task3",
                run_id=dag_run.run_id,
                session=session,
            )
        with conf_vars({("core", "enable_xcom_pickling"): "True"}):
            ret_value = XCom.get_value(key="xcom_test3", ti_key=ti_key, session=session)
        assert ret_value == {"key": "value"}

    def test_xcom_deserialize_with_pickle_to_json_switch(self, dag_run, session):
        with conf_vars({("core", "enable_xcom_pickling"): "True"}):
            XCom.set(
                key="xcom_test3",
                value={"key": "value"},
                dag_id=dag_run.dag_id,
                task_id="test_task3",
                run_id=dag_run.run_id,
                session=session,
            )
        with conf_vars({("core", "enable_xcom_pickling"): "False"}):
            ret_value = XCom.get_one(
                key="xcom_test3",
                dag_id=dag_run.dag_id,
                task_id="test_task3",
                run_id=dag_run.run_id,
                session=session,
            )
        assert ret_value == {"key": "value"}

    @conf_vars({("core", "xcom_enable_pickling"): "False"})
    def test_xcom_disable_pickle_type_fail_on_non_json(self, dag_run, session):
        class PickleRce:
            def __reduce__(self):
                return os.system, ("ls -alt",)

        with pytest.raises(TypeError):
            XCom.set(
                key="xcom_test3",
                value=PickleRce(),
                dag_id=dag_run.dag_id,
                task_id="test_task3",
                run_id=dag_run.run_id,
                session=session,
            )

    @mock.patch("airflow.models.xcom.XCom.orm_deserialize_value")
    def test_xcom_init_on_load_uses_orm_deserialize_value(self, mock_orm_deserialize):
        instance = BaseXCom(
            key="key",
            value="value",
            timestamp=timezone.utcnow(),
            execution_date=timezone.utcnow(),
            task_id="task_id",
            dag_id="dag_id",
        )
        instance.init_on_load()
        mock_orm_deserialize.assert_called_once_with()

    @conf_vars({("core", "xcom_backend"): "tests.models.test_xcom.CustomXCom"})
    def test_get_one_custom_backend_no_use_orm_deserialize_value(self, dag_run, session):
        """Test that XCom.get_one does not call orm_deserialize_value"""
        XCom = resolve_xcom_backend()
        XCom.set(
            key=XCOM_RETURN_KEY,
            value={"key": "value"},
            dag_id=dag_run.dag_id,
            task_id="test_task",
            run_id=dag_run.run_id,
            session=session,
        )

        value = XCom.get_one(
            dag_id=dag_run.dag_id,
            task_id="test_task",
            run_id=dag_run.run_id,
            session=session,
        )
        assert value == {"key": "value"}
        XCom.orm_deserialize_value.assert_not_called()

    @conf_vars({("core", "enable_xcom_pickling"): 'False'})
    @mock.patch('airflow.models.xcom.conf.getimport')
    def test_set_serialize_call_old_signature(self, get_import, session):
        """
        When XCom.serialize_value takes only param ``value``, other kwargs should be ignored.
        """
        serialize_watcher = MagicMock()

        class OldSignatureXCom(BaseXCom):
            @staticmethod
            def serialize_value(value, **kwargs):
                serialize_watcher(value=value, **kwargs)
                return json.dumps(value).encode('utf-8')

        get_import.return_value = OldSignatureXCom

        kwargs = dict(
            value={"my_xcom_key": "my_xcom_value"},
            key=XCOM_RETURN_KEY,
            dag_id="test_dag",
            task_id="test_task",
            run_id=IN_MEMORY_RUN_ID,
        )

        XCom = resolve_xcom_backend()
        XCom.set(**kwargs)
        serialize_watcher.assert_called_once_with(value=kwargs['value'])

    @conf_vars({("core", "enable_xcom_pickling"): 'False'})
    @mock.patch('airflow.models.xcom.conf.getimport')
    def test_set_serialize_call_current_signature(self, get_import, session):
        """
        When XCom.serialize_value includes params execution_date, key, dag_id, task_id and run_id,
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
                return json.dumps(value).encode('utf-8')

        get_import.return_value = CurrentSignatureXCom

        kwargs = dict(
            value={"my_xcom_key": "my_xcom_value"},
            key=XCOM_RETURN_KEY,
            dag_id="test_dag",
            task_id="test_task",
            run_id=IN_MEMORY_RUN_ID,
            map_index=-1,
        )
        expected = {**kwargs, 'run_id': '__airflow_in_memory_dagrun__'}
        XCom = resolve_xcom_backend()
        XCom.set(**kwargs)
        serialize_watcher.assert_called_once_with(**expected)


@pytest.fixture(
    params=[
        pytest.param("true", id="enable_xcom_pickling=true"),
        pytest.param("false", id="enable_xcom_pickling=false"),
    ],
)
def setup_xcom_pickling(request):
    with conf_vars({("core", "enable_xcom_pickling"): str(request.param)}):
        yield


@pytest.fixture()
def push_simple_json_xcom(session):
    def func(*, dag_run: DagRun, task_id: str, key: str, value):
        return XCom.set(
            key=key,
            value=value,
            dag_id=dag_run.dag_id,
            task_id=task_id,
            run_id=dag_run.run_id,
            session=session,
        )

    return func


@pytest.mark.usefixtures("setup_xcom_pickling")
class TestXComGet:
    @pytest.fixture()
    def setup_for_xcom_get_one(self, dag_run, push_simple_json_xcom):
        push_simple_json_xcom(dag_run=dag_run, task_id="task_id_1", key="xcom_1", value={"key": "value"})

    @pytest.mark.usefixtures("setup_for_xcom_get_one")
    def test_xcom_get_one(self, session, dag_run):
        stored_value = XCom.get_one(
            key="xcom_1",
            dag_id=dag_run.dag_id,
            task_id="task_id_1",
            run_id=dag_run.run_id,
            session=session,
        )
        assert stored_value == {"key": "value"}

    @pytest.mark.usefixtures("setup_for_xcom_get_one")
    def test_xcom_get_one_with_execution_date(self, session, dag_run):
        with pytest.deprecated_call():
            stored_value = XCom.get_one(
                key="xcom_1",
                dag_id=dag_run.dag_id,
                task_id="task_id_1",
                execution_date=dag_run.logical_date,
                session=session,
            )
        assert stored_value == {"key": "value"}

    @pytest.fixture()
    def dag_runs_for_xcom_get_one_from_prior_date(self, dag_run_factory, push_simple_json_xcom):
        date1 = timezone.datetime(2021, 12, 3, 4, 56)
        dr1 = dag_run_factory(dag_id="dag", execution_date=date1)
        dr2 = dag_run_factory(dag_id="dag", execution_date=date1 + datetime.timedelta(days=1))

        # The earlier run pushes an XCom, but not the later run, but the later
        # run can get this earlier XCom with ``include_prior_dates``.
        push_simple_json_xcom(dag_run=dr1, task_id="task_1", key="xcom_1", value={"key": "value"})

        return dr1, dr2

    def test_xcom_get_one_from_prior_date(self, session, dag_runs_for_xcom_get_one_from_prior_date):
        _, dr2 = dag_runs_for_xcom_get_one_from_prior_date
        retrieved_value = XCom.get_one(
            run_id=dr2.run_id,
            key="xcom_1",
            task_id="task_1",
            dag_id="dag",
            include_prior_dates=True,
            session=session,
        )
        assert retrieved_value == {"key": "value"}

    def test_xcom_get_one_from_prior_with_execution_date(
        self,
        session,
        dag_runs_for_xcom_get_one_from_prior_date,
    ):
        _, dr2 = dag_runs_for_xcom_get_one_from_prior_date
        with pytest.deprecated_call():
            retrieved_value = XCom.get_one(
                execution_date=dr2.execution_date,
                key="xcom_1",
                task_id="task_1",
                dag_id="dag",
                include_prior_dates=True,
                session=session,
            )
        assert retrieved_value == {"key": "value"}

    @pytest.fixture()
    def setup_for_xcom_get_many_single_argument_value(self, dag_run, push_simple_json_xcom):
        push_simple_json_xcom(dag_run=dag_run, task_id="task_id_1", key="xcom_1", value={"key": "value"})

    @pytest.mark.usefixtures("setup_for_xcom_get_many_single_argument_value")
    def test_xcom_get_many_single_argument_value(self, session, dag_run):
        stored_xcoms = XCom.get_many(
            key="xcom_1",
            dag_ids=dag_run.dag_id,
            task_ids="task_id_1",
            run_id=dag_run.run_id,
            session=session,
        ).all()
        assert len(stored_xcoms) == 1
        assert stored_xcoms[0].key == "xcom_1"
        assert stored_xcoms[0].value == {"key": "value"}

    @pytest.mark.usefixtures("setup_for_xcom_get_many_single_argument_value")
    def test_xcom_get_many_single_argument_value_with_execution_date(self, session, dag_run):
        with pytest.deprecated_call():
            stored_xcoms = XCom.get_many(
                execution_date=dag_run.logical_date,
                key="xcom_1",
                dag_ids=dag_run.dag_id,
                task_ids="task_id_1",
                session=session,
            ).all()
        assert len(stored_xcoms) == 1
        assert stored_xcoms[0].key == "xcom_1"
        assert stored_xcoms[0].value == {"key": "value"}

    @pytest.fixture()
    def setup_for_xcom_get_many_multiple_tasks(self, dag_run, push_simple_json_xcom):
        push_simple_json_xcom(dag_run=dag_run, key="xcom_1", value={"key1": "value1"}, task_id="task_id_1")
        push_simple_json_xcom(dag_run=dag_run, key="xcom_1", value={"key2": "value2"}, task_id="task_id_2")

    @pytest.mark.usefixtures("setup_for_xcom_get_many_multiple_tasks")
    def test_xcom_get_many_multiple_tasks(self, session, dag_run):
        stored_xcoms = XCom.get_many(
            key="xcom_1",
            dag_ids=dag_run.dag_id,
            task_ids=["task_id_1", "task_id_2"],
            run_id=dag_run.run_id,
            session=session,
        )
        sorted_values = [x.value for x in sorted(stored_xcoms, key=operator.attrgetter("task_id"))]
        assert sorted_values == [{"key1": "value1"}, {"key2": "value2"}]

    @pytest.mark.usefixtures("setup_for_xcom_get_many_multiple_tasks")
    def test_xcom_get_many_multiple_tasks_with_execution_date(self, session, dag_run):
        with pytest.deprecated_call():
            stored_xcoms = XCom.get_many(
                execution_date=dag_run.logical_date,
                key="xcom_1",
                dag_ids=dag_run.dag_id,
                task_ids=["task_id_1", "task_id_2"],
                session=session,
            )
        sorted_values = [x.value for x in sorted(stored_xcoms, key=operator.attrgetter("task_id"))]
        assert sorted_values == [{"key1": "value1"}, {"key2": "value2"}]

    @pytest.fixture()
    def dag_runs_for_xcom_get_many_from_prior_dates(self, dag_run_factory, push_simple_json_xcom):
        date1 = timezone.datetime(2021, 12, 3, 4, 56)
        date2 = date1 + datetime.timedelta(days=1)
        dr1 = dag_run_factory(dag_id="dag", execution_date=date1)
        dr2 = dag_run_factory(dag_id="dag", execution_date=date2)
        push_simple_json_xcom(dag_run=dr1, task_id="task_1", key="xcom_1", value={"key1": "value1"})
        push_simple_json_xcom(dag_run=dr2, task_id="task_1", key="xcom_1", value={"key2": "value2"})
        return dr1, dr2

    def test_xcom_get_many_from_prior_dates(self, session, dag_runs_for_xcom_get_many_from_prior_dates):
        dr1, dr2 = dag_runs_for_xcom_get_many_from_prior_dates
        stored_xcoms = XCom.get_many(
            run_id=dr2.run_id,
            key="xcom_1",
            dag_ids="dag",
            task_ids="task_1",
            include_prior_dates=True,
            session=session,
        )

        # The retrieved XComs should be ordered by logical date, latest first.
        assert [x.value for x in stored_xcoms] == [{"key2": "value2"}, {"key1": "value1"}]
        assert [x.execution_date for x in stored_xcoms] == [dr2.logical_date, dr1.logical_date]

    def test_xcom_get_many_from_prior_dates_with_execution_date(
        self,
        session,
        dag_runs_for_xcom_get_many_from_prior_dates,
    ):
        dr1, dr2 = dag_runs_for_xcom_get_many_from_prior_dates
        with pytest.deprecated_call():
            stored_xcoms = XCom.get_many(
                execution_date=dr2.execution_date,
                key="xcom_1",
                dag_ids="dag",
                task_ids="task_1",
                include_prior_dates=True,
                session=session,
            )

        # The retrieved XComs should be ordered by logical date, latest first.
        assert [x.value for x in stored_xcoms] == [{"key2": "value2"}, {"key1": "value1"}]
        assert [x.execution_date for x in stored_xcoms] == [dr2.logical_date, dr1.logical_date]


@pytest.mark.usefixtures("setup_xcom_pickling")
class TestXComSet:
    def test_xcom_set(self, session, dag_run):
        XCom.set(
            key="xcom_1",
            value={"key": "value"},
            dag_id=dag_run.dag_id,
            task_id="task_1",
            run_id=dag_run.run_id,
            session=session,
        )
        stored_xcoms = session.query(XCom).all()
        assert stored_xcoms[0].key == "xcom_1"
        assert stored_xcoms[0].value == {"key": "value"}
        assert stored_xcoms[0].dag_id == "dag"
        assert stored_xcoms[0].task_id == "task_1"
        assert stored_xcoms[0].execution_date == dag_run.logical_date

    def test_xcom_set_with_execution_date(self, session, dag_run):
        with pytest.deprecated_call():
            XCom.set(
                key="xcom_1",
                value={"key": "value"},
                dag_id=dag_run.dag_id,
                task_id="task_1",
                execution_date=dag_run.execution_date,
                session=session,
            )
        stored_xcoms = session.query(XCom).all()
        assert stored_xcoms[0].key == "xcom_1"
        assert stored_xcoms[0].value == {"key": "value"}
        assert stored_xcoms[0].dag_id == "dag"
        assert stored_xcoms[0].task_id == "task_1"
        assert stored_xcoms[0].execution_date == dag_run.logical_date

    @pytest.fixture()
    def setup_for_xcom_set_again_replace(self, dag_run, push_simple_json_xcom):
        push_simple_json_xcom(dag_run=dag_run, task_id="task_1", key="xcom_1", value={"key1": "value1"})

    @pytest.mark.usefixtures("setup_for_xcom_set_again_replace")
    def test_xcom_set_again_replace(self, session, dag_run):
        assert session.query(XCom).one().value == {"key1": "value1"}
        XCom.set(
            key="xcom_1",
            value={"key2": "value2"},
            dag_id=dag_run.dag_id,
            task_id="task_1",
            run_id=dag_run.run_id,
            session=session,
        )
        assert session.query(XCom).one().value == {"key2": "value2"}

    @pytest.mark.usefixtures("setup_for_xcom_set_again_replace")
    def test_xcom_set_again_replace_with_execution_date(self, session, dag_run):
        assert session.query(XCom).one().value == {"key1": "value1"}
        with pytest.deprecated_call():
            XCom.set(
                key="xcom_1",
                value={"key2": "value2"},
                dag_id=dag_run.dag_id,
                task_id="task_1",
                execution_date=dag_run.logical_date,
                session=session,
            )
        assert session.query(XCom).one().value == {"key2": "value2"}


@pytest.mark.usefixtures("setup_xcom_pickling")
class TestXComClear:
    @pytest.fixture()
    def setup_for_xcom_clear(self, dag_run, push_simple_json_xcom):
        push_simple_json_xcom(dag_run=dag_run, task_id="task_1", key="xcom_1", value={"key": "value"})

    @pytest.mark.usefixtures("setup_for_xcom_clear")
    def test_xcom_clear(self, session, dag_run):
        assert session.query(XCom).count() == 1
        XCom.clear(
            dag_id=dag_run.dag_id,
            task_id="task_1",
            run_id=dag_run.run_id,
            session=session,
        )
        assert session.query(XCom).count() == 0

    @pytest.mark.usefixtures("setup_for_xcom_clear")
    def test_xcom_clear_with_execution_date(self, session, dag_run):
        assert session.query(XCom).count() == 1
        with pytest.deprecated_call():
            XCom.clear(
                dag_id=dag_run.dag_id,
                task_id="task_1",
                execution_date=dag_run.execution_date,
                session=session,
            )
        assert session.query(XCom).count() == 0

    @pytest.mark.usefixtures("setup_for_xcom_clear")
    def test_xcom_clear_different_run(self, session, dag_run):
        XCom.clear(
            dag_id=dag_run.dag_id,
            task_id="task_1",
            run_id="different_run",
            session=session,
        )
        assert session.query(XCom).count() == 1

    @pytest.mark.usefixtures("setup_for_xcom_clear")
    def test_xcom_clear_different_execution_date(self, session, dag_run):
        with pytest.deprecated_call():
            XCom.clear(
                dag_id=dag_run.dag_id,
                task_id="task_1",
                execution_date=timezone.utcnow(),
                session=session,
            )
        assert session.query(XCom).count() == 1
