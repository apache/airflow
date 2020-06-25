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

from mock import patch

from airflow.www import app

MOCK_CONF = {
    'core': {
        'parallelism': ('1024', 'env var'),
    },
    'smtp': {
        'smtp_host': ('localhost', 'default'),
        'smtp_mail_from': ('airflow@example.com', 'airflow.cfg'),
    },
}


@patch(
    "airflow.api_connexion.endpoints.config_endpoint.conf.as_dict",
    return_value=MOCK_CONF
)
class TestGetConfig:
    @classmethod
    def setup_class(cls) -> None:
        cls.app = app.create_app(testing=True)  # type:ignore

    def setup_method(self) -> None:
        self.client = self.app.test_client()  # type:ignore

    def test_should_response_200_text_plain(self, mock_as_dict):
        response = self.client.get("/api/v1/config", headers={'Accept': 'text/plain'})
        assert response.status_code == 200
        expected = textwrap.dedent("""\
        [core]
        parallelism = 1024  # source: env var

        [smtp]
        smtp_host = localhost  # source: default
        smtp_mail_from = airflow@example.com  # source: airflow.cfg
        """)
        assert expected == response.data.decode()

    def test_should_response_200_application_json(self, mock_as_dict):
        response = self.client.get("/api/v1/config", headers={'Accept': 'application/json'})
        assert response.status_code == 200
        expected = {
            'sections': [
                {
                    'name': 'core',
                    'options': [
                        {'key': 'parallelism', 'value': '1024', 'source': 'env var'},
                    ]
                },
                {
                    'name': 'smtp',
                    'options': [
                        {'key': 'smtp_host', 'value': 'localhost', 'source': 'default'},
                        {'key': 'smtp_mail_from', 'value': 'airflow@example.com', 'source': 'airflow.cfg'},
                    ]
                },
            ]
        }
        assert expected == response.data.json()

    def test_should_response_406(self, mock_as_dict):
        response = self.client.get("/api/v1/config", headers={'Accept': 'application/octet-stream'})
        assert response.status_code == 406
