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
from typing import TYPE_CHECKING

import pytest
from itsdangerous import TimedJSONWebSignatureSerializer

from airflow.configuration import conf
from airflow.utils.serve_logs import flask_app
from tests.test_utils.config import conf_vars

if TYPE_CHECKING:
    from flask.testing import FlaskClient

LOG_DATA = "Airflow log data" * 20


@pytest.fixture
def client(tmpdir):
    with conf_vars({('logging', 'base_log_folder'): str(tmpdir)}):
        app = flask_app()

        yield app.test_client()


@pytest.fixture
def sample_log(tmpdir):
    f = tmpdir / 'sample.log'
    f.write(LOG_DATA.encode())

    return f


@pytest.fixture
def signer():
    return TimedJSONWebSignatureSerializer(
        secret_key=conf.get('webserver', 'secret_key'),
        algorithm_name='HS512',
        expires_in=30,
        # This isn't really a "salt", more of a signing context
        salt='task-instance-logs',
    )


@pytest.mark.usefixtures('sample_log')
class TestServeLogs:
    def test_forbidden_no_auth(self, client: "FlaskClient"):
        assert 403 == client.get('/log/sample.log').status_code

    def test_should_serve_file(self, client: "FlaskClient", signer):
        assert (
            LOG_DATA
            == client.get(
                '/log/sample.log',
                headers={
                    'Authorization': signer.dumps('sample.log'),
                },
            ).data.decode()
        )

    def test_forbidden_too_long_validity(self, client: "FlaskClient", signer):
        signer.expires_in = 3600
        assert (
            403
            == client.get(
                '/log/sample.log',
                headers={
                    'Authorization': signer.dumps('sample.log'),
                },
            ).status_code
        )

    def test_forbidden_expired(self, client: "FlaskClient", signer):
        # Fake the time we think we are
        signer.now = lambda: 0
        assert (
            403
            == client.get(
                '/log/sample.log',
                headers={
                    'Authorization': signer.dumps('sample.log'),
                },
            ).status_code
        )

    def test_wrong_context(self, client: "FlaskClient", signer):
        signer.salt = None
        assert (
            403
            == client.get(
                '/log/sample.log',
                headers={
                    'Authorization': signer.dumps('sample.log'),
                },
            ).status_code
        )
