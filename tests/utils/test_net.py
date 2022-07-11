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
import re
import unittest
from unittest import mock

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.utils import net
from tests.test_utils.config import conf_vars


def get_hostname():
    return 'awesomehostname'


class TestGetHostname(unittest.TestCase):
    @mock.patch('airflow.utils.net.getfqdn', return_value='first')
    @conf_vars({('core', 'hostname_callable'): None})
    def test_get_hostname_unset(self, mock_getfqdn):
        assert 'first' == net.get_hostname()

    @conf_vars({('core', 'hostname_callable'): 'tests.utils.test_net.get_hostname'})
    def test_get_hostname_set(self):
        assert 'awesomehostname' == net.get_hostname()

    @conf_vars({('core', 'hostname_callable'): 'tests.utils.test_net'})
    def test_get_hostname_set_incorrect(self):
        with pytest.raises(TypeError):
            net.get_hostname()

    @conf_vars({('core', 'hostname_callable'): 'tests.utils.test_net.missing_func'})
    def test_get_hostname_set_missing(self):
        with pytest.raises(
            AirflowConfigException,
            match=re.escape(
                'The object could not be loaded. Please check "hostname_callable" key in "core" section. '
                'Current value: "tests.utils.test_net.missing_func"'
            ),
        ):
            net.get_hostname()
