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
import textwrap
import unittest
from unittest import mock

import pytest

from airflow.www import app

FAKE_CONFIG_DATA = {
    'smtp': {
        'smtp_host': ('localhost', 'airflow.cfg'),
        'smtp_starttls': ('True', 'airflow.cfg'),
        'smtp_ssl': ('False', 'airflow.cfg'),
        'smtp_port': ('25', 'airflow.cfg'),
        'smtp_mail_from': ('airflow@example.com', 'airflow.cfg')
    },
}


class TestGetConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore

    @mock.patch(
        "airflow.api_connexion.endpoints.config_endpoint.conf.as_dict",
        return_value=FAKE_CONFIG_DATA
    )
    def test_should_response_200_text_plain(self, mock_as_dict):
        response = self.client.get("/api/v1/config", headers={'Accept': 'text/plain'})
        assert response.status_code == 200
        expected_response = textwrap.dedent("""\
        [smtp]
        smtp_host = localhost  # source: airflow.cfg
        smtp_starttls = True  # source: airflow.cfg
        smtp_ssl = False  # source: airflow.cfg
        smtp_port = 25  # source: airflow.cfg
        smtp_mail_from = airflow@example.com  # source: airflow.cfg
        """)
        assert expected_response == response.data.decode()

    @mock.patch(
        "airflow.api_connexion.endpoints.config_endpoint.conf.as_dict",
        return_value=FAKE_CONFIG_DATA
    )
    def test_should_response_200_application_json(self, mock_as_dict):
        response = self.client.get("/api/v1/config", headers={'Accept': 'application/json'})
        assert response.status_code == 200
        expected_response = {}
        assert expected_response == response.data.json()

    @mock.patch(
        "airflow.api_connexion.endpoints.config_endpoint.conf.as_dict",
        return_value=FAKE_CONFIG_DATA
    )
    def test_should_response_409(self, mock_as_dict):
        response = self.client.get("/api/v1/config", headers={'Accept': 'application/octet-stream'})
        assert response.status_code == 406
