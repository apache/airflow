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

import json
from unittest import mock

from airflow.providers.yandex.utils.credentials import (
    get_credentials,
    get_service_account_id,
    get_service_account_key,
)


def test_get_credentials_oauth_token():
    oauth_token = "y3_Vd3eub7w9bIut67GHeL345gfb5GAnd3dZnf08FR1vjeUFve7Yi8hGvc"
    service_account_key = {
        "id": "...",
        "service_account_id": "...",
        "private_key": "...",
    }
    service_account_key_json = json.dumps(service_account_key)
    service_account_file_path = "/home/airflow/authorized_key.json"
    expected = {"token": oauth_token}

    res = get_credentials(
        oauth_token=oauth_token,
        service_account_json=service_account_key_json,
        service_account_json_path=service_account_file_path,
    )

    assert res == expected


@mock.patch("airflow.providers.yandex.utils.credentials.get_service_account_key")
def test_get_credentials_service_account_key(mock_get_service_account_key):
    service_account_key = {
        "id": "...",
        "service_account_id": "...",
        "private_key": "...",
    }
    service_account_key_json = json.dumps(service_account_key)
    service_account_file_path = "/home/airflow/authorized_key.json"
    expected = {"service_account_key": service_account_key}

    mock_get_service_account_key.return_value = service_account_key

    res = get_credentials(
        service_account_json=service_account_key_json,
        service_account_json_path=service_account_file_path,
    )

    assert res == expected


def test_get_credentials_metadata_service(caplog):
    expected = {}

    res = get_credentials()

    assert res == expected
    assert "using metadata service as credentials" in caplog.text


def test_get_service_account_key():
    service_account_key = {
        "id": "...",
        "service_account_id": "...",
        "private_key": "...",
    }
    service_account_key_json = json.dumps(service_account_key)
    expected = service_account_key

    res = get_service_account_key(
        service_account_json=service_account_key_json,
    )

    assert res == expected


def test_get_service_account_dict():
    service_account_key = {
        "id": "...",
        "service_account_id": "...",
        "private_key": "...",
    }
    expected = service_account_key

    res = get_service_account_key(
        service_account_json=service_account_key,
    )

    assert res == expected


def test_get_service_account_key_file(tmp_path):
    service_account_key = {
        "id": "...",
        "service_account_id": "...",
        "private_key": "...",
    }
    service_account_key_json = json.dumps(service_account_key)
    service_account_file = tmp_path / "authorized_key.json"
    service_account_file.write_text(service_account_key_json)
    service_account_file_path = str(service_account_file)
    expected = service_account_key

    res = get_service_account_key(
        service_account_json=service_account_key_json,
        service_account_json_path=service_account_file_path,
    )

    assert res == expected


def test_get_service_account_key_none():
    expected = None

    res = get_service_account_key()

    assert res == expected


@mock.patch("airflow.providers.yandex.utils.credentials.get_service_account_key")
def test_get_service_account_id(mock_get_service_account_key):
    service_account_id = "this_is_service_account_id"
    service_account_key = {
        "id": "...",
        "service_account_id": service_account_id,
        "private_key": "...",
    }
    service_account_key_json = json.dumps(service_account_key)
    service_account_file_path = "/home/airflow/authorized_key.json"
    expected = service_account_id

    mock_get_service_account_key.return_value = service_account_key

    res = get_service_account_id(
        service_account_json=service_account_key_json,
        service_account_json_path=service_account_file_path,
    )

    assert res == expected


@mock.patch("airflow.providers.yandex.utils.credentials.get_service_account_key")
def test_get_service_account_id_none(mock_get_service_account_key):
    expected = None

    mock_get_service_account_key.return_value = None

    res = get_service_account_id()

    assert res == expected
