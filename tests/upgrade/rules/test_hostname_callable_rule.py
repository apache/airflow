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

from airflow.upgrade.rules.hostname_callable_rule import HostnameCallable
from tests.test_utils.config import conf_vars


class TestFernetEnabledRule(TestCase):
    @conf_vars({("core", "hostname_callable"): "dummyhostname:function"})
    def test_incorrect_hostname(self):
        result = HostnameCallable().check()
        self.assertEqual(
            result,
            "Error: hostname_callable `dummyhostname:function` "
            "contains a colon instead of a dot. please change to `dummyhostname.function`",
        )

    @conf_vars({("core", "hostname_callable"): "dummyhostname.function"})
    def test_correct_hostname(self):
        result = HostnameCallable().check()
        self.assertEqual(result, None)
