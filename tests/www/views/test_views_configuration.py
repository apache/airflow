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
import os
from unittest import mock

import pytest

from airflow import configuration
from tests.test_utils.config import conf_vars


@pytest.fixture(scope="module", autouse=True)
def init_config():
    with mock.patch.dict(os.environ, {"AIRFLOW__CORE__UNIT_TEST_MODE": "False"}):
        configuration.conf = configuration.initialize_config()
    yield
    configuration.conf = configuration.initialize_config()


def test_configuration_do_not_expose_config(admin_client, checker):
    with conf_vars({('webserver', 'expose_config'): 'False'}):
        resp = admin_client.get('configuration', follow_redirects=True)
    checker.check_content_in_response(
        [
            'Airflow Configuration',
            '# Your Airflow administrator chose not to expose the configuration, '
            'most likely for security reasons.',
        ],
        resp,
    )


def test_configuration_expose_config(admin_client, checker):
    with conf_vars({('webserver', 'expose_config'): 'True'}):
        resp = admin_client.get('configuration', follow_redirects=True)
    checker.check_content_in_response(['Airflow Configuration', 'Running Configuration'], resp)
