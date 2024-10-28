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
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import jwt
import pytest
import time_machine

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.utils import timezone
from airflow.utils.jwt_signer import JWTSigner
from airflow.utils.serve_logs import create_app

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from flask.testing import FlaskClient

LOG_DATA = "Airflow log data" * 20


@pytest.fixture
def client_without_config(tmp_path):
    with conf_vars({("logging", "base_log_folder"): tmp_path.as_posix()}):
        app = create_app()

        yield app.test_client()


@pytest.fixture
def client_with_config():
    with conf_vars(
        {
            (
                "logging",
                "logging_config_class",
            ): "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG"
        }
    ):
        app = create_app()

        yield app.test_client()


@pytest.fixture(params=["client_without_config", "client_with_config"])
def client(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def sample_log(request, tmp_path):
    client = request.getfixturevalue("client")

    if client == request.getfixturevalue("client_without_config"):
        base_log_dir = tmp_path
    elif client == request.getfixturevalue("client_with_config"):
        base_log_dir = Path(DEFAULT_LOGGING_CONFIG["handlers"]["task"]["base_log_folder"])
    else:
        raise ValueError(f"Unknown client fixture: {client}")

    f = base_log_dir.joinpath("sample.log")
    f.write_text(LOG_DATA)
    return f


@pytest.fixture
def signer(secret_key):
    return JWTSigner(
        secret_key=secret_key,
        expiration_time_in_seconds=30,
        audience="task-instance-logs",
    )


@pytest.fixture
def different_audience(secret_key):
    return JWTSigner(
        secret_key=secret_key,
        expiration_time_in_seconds=30,
        audience="different-audience",
    )


@pytest.mark.usefixtures("sample_log")
class TestServeLogs:
    def test_forbidden_no_auth(self, client: FlaskClient):
        assert 403 == client.get("/log/sample.log").status_code

    def test_should_serve_file(self, client: FlaskClient, signer):
        response = client.get(
            "/log/sample.log",
            headers={
                "Authorization": signer.generate_signed_token({"filename": "sample.log"}),
            },
        )
        assert response.data.decode() == LOG_DATA
        assert response.status_code == 200

    def test_forbidden_different_logname(self, client: FlaskClient, signer):
        response = client.get(
            "/log/sample.log",
            headers={
                "Authorization": signer.generate_signed_token(
                    {"filename": "different.log"}
                ),
            },
        )
        assert response.status_code == 403

    def test_forbidden_expired(self, client: FlaskClient, signer):
        with time_machine.travel("2010-01-14"):
            token = signer.generate_signed_token({"filename": "sample.log"})
        assert (
            client.get(
                "/log/sample.log",
                headers={
                    "Authorization": token,
                },
            ).status_code
            == 403
        )

    def test_forbidden_future(self, client: FlaskClient, signer):
        with time_machine.travel(timezone.utcnow() + timedelta(seconds=3600)):
            token = signer.generate_signed_token({"filename": "sample.log"})
        assert (
            client.get(
                "/log/sample.log",
                headers={
                    "Authorization": token,
                },
            ).status_code
            == 403
        )

    def test_ok_with_short_future_skew(self, client: FlaskClient, signer):
        with time_machine.travel(timezone.utcnow() + timedelta(seconds=1)):
            token = signer.generate_signed_token({"filename": "sample.log"})
        assert (
            client.get(
                "/log/sample.log",
                headers={
                    "Authorization": token,
                },
            ).status_code
            == 200
        )

    def test_ok_with_short_past_skew(self, client: FlaskClient, signer):
        with time_machine.travel(timezone.utcnow() - timedelta(seconds=31)):
            token = signer.generate_signed_token({"filename": "sample.log"})
        assert (
            client.get(
                "/log/sample.log",
                headers={
                    "Authorization": token,
                },
            ).status_code
            == 200
        )

    def test_forbidden_with_long_future_skew(self, client: FlaskClient, signer):
        with time_machine.travel(timezone.utcnow() + timedelta(seconds=10)):
            token = signer.generate_signed_token({"filename": "sample.log"})
        assert (
            client.get(
                "/log/sample.log",
                headers={
                    "Authorization": token,
                },
            ).status_code
            == 403
        )

    def test_forbidden_with_long_past_skew(self, client: FlaskClient, signer):
        with time_machine.travel(timezone.utcnow() - timedelta(seconds=40)):
            token = signer.generate_signed_token({"filename": "sample.log"})
        assert (
            client.get(
                "/log/sample.log",
                headers={
                    "Authorization": token,
                },
            ).status_code
            == 403
        )

    def test_wrong_audience(self, client: FlaskClient, different_audience):
        assert (
            client.get(
                "/log/sample.log",
                headers={
                    "Authorization": different_audience.generate_signed_token(
                        {"filename": "sample.log"}
                    ),
                },
            ).status_code
            == 403
        )

    @pytest.mark.parametrize("claim_to_remove", ["iat", "exp", "nbf", "aud"])
    def test_missing_claims(self, claim_to_remove: str, client: FlaskClient, secret_key):
        jwt_dict = {
            "aud": "task-instance-logs",
            "iat": timezone.utcnow(),
            "nbf": timezone.utcnow(),
            "exp": timezone.utcnow() + timedelta(seconds=30),
        }
        del jwt_dict[claim_to_remove]
        jwt_dict.update({"filename": "sample.log"})
        token = jwt.encode(
            jwt_dict,
            secret_key,
            algorithm="HS512",
        )
        assert (
            client.get(
                "/log/sample.log",
                headers={
                    "Authorization": token,
                },
            ).status_code
            == 403
        )
