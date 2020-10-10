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

from airflow.models.baseoperator import BaseOperatorMeta
from tests.test_utils.mock_operators import MockOperator
from airflow.upgrade.rules.customoperator_uses_metaclass import CustomOperatorUsesMetaclassRule


class TestCustomOperatorUsesMetaclassRule(TestCase):
    def test_check(self):
        rule = CustomOperatorUsesMetaclassRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        mock_operator = MockOperator()

        assert isinstance(mock_operator, BaseOperatorMeta)

        msgs = rule.check()
        assert [m for m in msgs if "CustomOperatorUsesMetaclassRule" in m], \
            "CustomOperatorUsesMetaclassRule not in warning messages"
