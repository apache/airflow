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

from airflow.upgrade.rules.fernet_enabled import FernetEnabledRule
from tests.test_utils.config import conf_vars


class TestFernetEnabledRule(TestCase):
    @conf_vars({("core", "fernet_key"): ""})
    def test_invalid_check(self):
        rule = FernetEnabledRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        msg = (
            "fernet_key in airflow.cfg must be explicitly set empty as fernet mechanism is enabled"
            "by default. This means that the apache-airflow[crypto] extra-packages are always installed."
            "However, this requires that your operating system has libffi-dev installed."
        )
        response = rule.check()
        assert response == msg

    @conf_vars({("core", "fernet_key"): "dummyfernet"})
    def test_valid_check(self):
        rule = FernetEnabledRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        response = rule.check()
        assert response is None
