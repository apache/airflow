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
import os
from unittest import mock

import pytest

from airflow.configuration import conf
from airflow.models.xcom import XCOM_RETURN_KEY, BaseXCom, XCom, resolve_xcom_backend
from airflow.utils import timezone
from tests.test_utils.config import conf_vars


class CustomXCom(BaseXCom):
    def orm_deserialize_value(self):
        return 'Short value...'


class TestXCom:
    @conf_vars({("core", "xcom_backend"): "tests.models.test_xcom.CustomXCom"})
    def test_resolve_xcom_class(self):
        cls = resolve_xcom_backend()
        assert issubclass(cls, CustomXCom)

    @conf_vars({("core", "xcom_backend"): "", ("core", "enable_xcom_pickling"): "False"})
    def test_resolve_xcom_class_fallback_to_basexcom(self):
        cls = resolve_xcom_backend()
        assert issubclass(cls, BaseXCom)
        assert cls().serialize_value([1]) == b"[1]"

    @conf_vars({("core", "enable_xcom_pickling"): "False"})
    @conf_vars({("core", "xcom_backend"): "to be removed"})
    def test_resolve_xcom_class_fallback_to_basexcom_no_config(self):
        conf.remove_option("core", "xcom_backend")
        cls = resolve_xcom_backend()
        assert issubclass(cls, BaseXCom)
        assert cls().serialize_value([1]) == b"[1]"

    @pytest.mark.parametrize(
        ("enable_xcom_pickling",),
        [
            pytest.param(True, id='enable_xcom_pickling=True'),
            pytest.param(False, id='enable_xcom_pickling=False'),
        ],
    )
    def test_xcom_get_one_get_many(self, enable_xcom_pickling, session):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test1"
        dag_id = "test_dag1"
        task_id = "test_task1"

        with conf_vars({("core", "enable_xcom_pickling"): str(enable_xcom_pickling)}):
            XCom.set(
                key=key,
                value=json_obj,
                dag_id=dag_id,
                task_id=task_id,
                execution_date=execution_date,
                session=session,
            )

            ret_value = (
                XCom.get_many(
                    key=key, dag_ids=dag_id, task_ids=task_id, execution_date=execution_date, session=session
                )
                .first()
                .value
            )

            assert ret_value == json_obj

            ret_value = XCom.get_one(
                key=key, dag_id=dag_id, task_id=task_id, execution_date=execution_date, session=session
            )

            assert ret_value == json_obj

            ret_value = (
                session.query(XCom)
                .filter(
                    XCom.key == key,
                    XCom.dag_id == dag_id,
                    XCom.task_id == task_id,
                    XCom.execution_date == execution_date,
                )
                .first()
                .value
            )

            assert ret_value == json_obj

    def test_xcom_deserialize_with_json_to_pickle_switch(self, session):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test3"
        dag_id = "test_dag"
        task_id = "test_task3"

        with conf_vars({("core", "enable_xcom_pickling"): "False"}):
            XCom.set(
                key=key,
                value=json_obj,
                dag_id=dag_id,
                task_id=task_id,
                execution_date=execution_date,
                session=session,
            )

        with conf_vars({("core", "enable_xcom_pickling"): "True"}):
            ret_value = XCom.get_one(
                key=key, dag_id=dag_id, task_id=task_id, execution_date=execution_date, session=session
            )

        assert ret_value == json_obj

    def test_xcom_deserialize_with_pickle_to_json_switch(self, session):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test3"
        dag_id = "test_dag"
        task_id = "test_task3"

        with conf_vars({("core", "enable_xcom_pickling"): "True"}):
            XCom.set(
                key=key,
                value=json_obj,
                dag_id=dag_id,
                task_id=task_id,
                execution_date=execution_date,
                session=session,
            )

        with conf_vars({("core", "enable_xcom_pickling"): "False"}):
            ret_value = XCom.get_one(
                key=key, dag_id=dag_id, task_id=task_id, execution_date=execution_date, session=session
            )

        assert ret_value == json_obj

    @conf_vars({("core", "xcom_enable_pickling"): "False"})
    def test_xcom_disable_pickle_type_fail_on_non_json(self, session):
        class PickleRce:
            def __reduce__(self):
                return os.system, ("ls -alt",)

        with pytest.raises(TypeError):
            XCom.set(
                key="xcom_test3",
                value=PickleRce(),
                dag_id="test_dag3",
                task_id="test_task3",
                execution_date=timezone.utcnow(),
                session=session,
            )

    @conf_vars({("core", "xcom_enable_pickling"): "True"})
    def test_xcom_get_many(self, session):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test4"
        dag_id1 = "test_dag4"
        task_id1 = "test_task4"
        dag_id2 = "test_dag5"
        task_id2 = "test_task5"

        XCom.set(
            key=key,
            value=json_obj,
            dag_id=dag_id1,
            task_id=task_id1,
            execution_date=execution_date,
            session=session,
        )

        XCom.set(
            key=key,
            value=json_obj,
            dag_id=dag_id2,
            task_id=task_id2,
            execution_date=execution_date,
            session=session,
        )

        results = XCom.get_many(key=key, execution_date=execution_date, session=session)

        for result in results:
            assert result.value == json_obj

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
    def test_get_one_doesnt_use_orm_deserialize_value(self, session):
        """Test that XCom.get_one does not call orm_deserialize_value"""
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = XCOM_RETURN_KEY
        dag_id = "test_dag"
        task_id = "test_task"

        XCom = resolve_xcom_backend()
        XCom.set(
            key=key,
            value=json_obj,
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
            session=session,
        )

        value = XCom.get_one(dag_id=dag_id, task_id=task_id, execution_date=execution_date, session=session)

        assert value == json_obj
