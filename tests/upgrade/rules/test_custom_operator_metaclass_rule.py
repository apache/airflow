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
from unittest import TestCase

from airflow.models.baseoperator import BaseOperator
from airflow.upgrade.rules.custom_operator_metaclass_rule import (
    BaseOperatorMetaclassRule,
    check_task_for_metaclasses,
)
from six import with_metaclass


class MyMeta(type):
    pass


class MyMetaOperator(with_metaclass(MyMeta, BaseOperator)):
    def execute(self, context):
        pass


class TestBaseOperatorMetaclassRule(TestCase):
    def test_individual_task(self):
        task = MyMetaOperator(task_id="foo")
        res = check_task_for_metaclasses(task)
        expected_error = (
            "Class <class 'tests.upgrade.rules.test_custom_operator_metaclass_rule.MyMetaOperator'> "
            "contained invalid custom metaclass <class "
            "'tests.upgrade.rules.test_custom_operator_metaclass_rule.MyMeta'>. "
            "Custom metaclasses for operators are not allowed in Airflow 2.0. "
            "Please remove this custom metaclass."
        )
        self.assertEqual(expected_error, res)

    def test_check(self):
        rule = BaseOperatorMetaclassRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)
        msgs = list(rule.check())
        self.assertEqual(msgs, [])
