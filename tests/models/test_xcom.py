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
import unittest

from airflow import settings
from airflow.configuration import conf
from airflow.models.xcom import BaseXCom, XCom, resolve_xcom_backend
from airflow.utils import timezone
from tests.test_utils import db
from tests.test_utils.config import conf_vars


class CustomXCom(BaseXCom):
    @staticmethod
    def serialize_value(_):
        return "custom_value"


class TestXCom(unittest.TestCase):

    def setUp(self) -> None:
        db.clear_db_xcom()

    def tearDown(self) -> None:
        db.clear_db_xcom()

    @conf_vars({("core", "xcom_backend"): "tests.models.test_xcom.CustomXCom"})
    def test_resolve_xcom_class(self):
        cls = resolve_xcom_backend()
        assert issubclass(cls, CustomXCom)
        assert cls().serialize_value(None) == "custom_value"

    @conf_vars(
        {("core", "xcom_backend"): "", ("core", "enable_xcom_pickling"): "False"}
    )
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

    @conf_vars({("core", "enable_xcom_pickling"): "False"})
    def test_xcom_disable_pickle_type(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test1"
        dag_id = "test_dag1"
        task_id = "test_task1"
        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id,
                 task_id=task_id,
                 execution_date=execution_date)

        ret_value = XCom.get_many(key=key,
                                  dag_ids=dag_id,
                                  task_ids=task_id,
                                  execution_date=execution_date).first().value

        self.assertEqual(ret_value, json_obj)

        session = settings.Session()
        ret_value = session.query(XCom).filter(XCom.key == key, XCom.dag_id == dag_id,
                                               XCom.task_id == task_id,
                                               XCom.execution_date == execution_date
                                               ).first().value

        self.assertEqual(ret_value, json_obj)

    @conf_vars({("core", "enable_xcom_pickling"): "False"})
    def test_xcom_get_one_disable_pickle_type(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test1"
        dag_id = "test_dag1"
        task_id = "test_task1"
        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id,
                 task_id=task_id,
                 execution_date=execution_date)

        ret_value = XCom.get_one(key=key,
                                 dag_id=dag_id,
                                 task_id=task_id,
                                 execution_date=execution_date)

        self.assertEqual(ret_value, json_obj)

        session = settings.Session()
        ret_value = session.query(XCom).filter(XCom.key == key, XCom.dag_id == dag_id,
                                               XCom.task_id == task_id,
                                               XCom.execution_date == execution_date
                                               ).first().value

        self.assertEqual(ret_value, json_obj)

    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_xcom_enable_pickle_type(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test2"
        dag_id = "test_dag2"
        task_id = "test_task2"
        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id,
                 task_id=task_id,
                 execution_date=execution_date)

        ret_value = XCom.get_many(key=key,
                                  dag_ids=dag_id,
                                  task_ids=task_id,
                                  execution_date=execution_date).first().value

        self.assertEqual(ret_value, json_obj)

        session = settings.Session()
        ret_value = session.query(XCom).filter(XCom.key == key, XCom.dag_id == dag_id,
                                               XCom.task_id == task_id,
                                               XCom.execution_date == execution_date
                                               ).first().value

        self.assertEqual(ret_value, json_obj)

    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_xcom_get_one_enable_pickle_type(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test3"
        dag_id = "test_dag"
        task_id = "test_task3"
        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id,
                 task_id=task_id,
                 execution_date=execution_date)

        ret_value = XCom.get_one(key=key,
                                 dag_id=dag_id,
                                 task_id=task_id,
                                 execution_date=execution_date)

        self.assertEqual(ret_value, json_obj)

        session = settings.Session()
        ret_value = session.query(XCom).filter(XCom.key == key, XCom.dag_id == dag_id,
                                               XCom.task_id == task_id,
                                               XCom.execution_date == execution_date
                                               ).first().value

        self.assertEqual(ret_value, json_obj)

    @conf_vars({("core", "xcom_enable_pickling"): "False"})
    def test_xcom_disable_pickle_type_fail_on_non_json(self):
        class PickleRce:
            def __reduce__(self):
                return os.system, ("ls -alt",)

        self.assertRaises(TypeError, XCom.set,
                          key="xcom_test3",
                          value=PickleRce(),
                          dag_id="test_dag3",
                          task_id="test_task3",
                          execution_date=timezone.utcnow())

    @conf_vars({("core", "xcom_enable_pickling"): "True"})
    def test_xcom_get_many(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test4"
        dag_id1 = "test_dag4"
        task_id1 = "test_task4"
        dag_id2 = "test_dag5"
        task_id2 = "test_task5"

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id1,
                 task_id=task_id1,
                 execution_date=execution_date)

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id2,
                 task_id=task_id2,
                 execution_date=execution_date)

        results = XCom.get_many(key=key, execution_date=execution_date)

        for result in results:
            self.assertEqual(result.value, json_obj)
