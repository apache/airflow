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
import configparser
import os
import unittest

from parameterized import parameterized

from tests.test_utils import AIRFLOW_MAIN_FOLDER

CONFIG_TEMPLATES_FOLDER = os.path.join(AIRFLOW_MAIN_FOLDER, "airflow", "config_templates")

DEFAULT_AIRFLOW_SECTIONS = [
    'core',
    "logging",
    "metrics",
    'secrets',
    'cli',
    'debug',
    'api',
    'lineage',
    'atlas',
    'operators',
    'hive',
    'webserver',
    'email',
    'smtp',
    'sentry',
    'celery_kubernetes_executor',
    'celery',
    'celery_broker_transport_options',
    'dask',
    'scheduler',
    'ldap',
    'kerberos',
    'github_enterprise',
    'admin',
    'elasticsearch',
    'elasticsearch_configs',
    'kubernetes',
    'smart_sensor',
]

DEFAULT_TEST_SECTIONS = [
    'core',
    "logging",
    'cli',
    'api',
    'operators',
    'hive',
    'webserver',
    'email',
    'smtp',
    'celery',
    'scheduler',
    'admin',
    'elasticsearch',
    'elasticsearch_configs',
    'kubernetes',
]


class TestAirflowCfg(unittest.TestCase):
    @parameterized.expand(
        [
            ("default_airflow.cfg",),
            ("default_test.cfg",),
        ]
    )
    def test_should_be_ascii_file(self, filename: str):
        with open(os.path.join(CONFIG_TEMPLATES_FOLDER, filename), "rb") as f:
            content = f.read().decode("ascii")
        self.assertTrue(content)

    @parameterized.expand(
        [
            (
                "default_airflow.cfg",
                DEFAULT_AIRFLOW_SECTIONS,
            ),
            (
                "default_test.cfg",
                DEFAULT_TEST_SECTIONS,
            ),
        ]
    )
    def test_should_be_ini_file(self, filename: str, expected_sections):
        filepath = os.path.join(CONFIG_TEMPLATES_FOLDER, filename)
        config = configparser.ConfigParser()
        config.read(filepath)

        self.assertEqual(expected_sections, config.sections())
