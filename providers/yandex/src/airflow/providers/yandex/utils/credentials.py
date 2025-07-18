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
import logging
from typing import TypedDict

log = logging.getLogger(__name__)


class CredentialsType(TypedDict, total=False):
    """Credentials dict description."""

    token: str
    service_account_key: dict[str, str]


def get_credentials(
    oauth_token: str | None = None,
    service_account_json: dict | str | None = None,
    service_account_json_path: str | None = None,
) -> CredentialsType:
    """
    Return credentials JSON for Yandex Cloud SDK based on credentials.

    Credentials will be used with this priority:

    * OAuth Token
    * Service Account JSON file
    * Service Account JSON
    * Metadata Service

    :param oauth_token: OAuth Token
    :param service_account_json: Service Account JSON key or dict
    :param service_account_json_path: Service Account JSON key file path
    :return: Credentials JSON
    """
    if oauth_token:
        return {"token": oauth_token}

    service_account_key = get_service_account_key(
        service_account_json=service_account_json,
        service_account_json_path=service_account_json_path,
    )
    if service_account_key:
        return {"service_account_key": service_account_key}

    log.info("using metadata service as credentials")
    return {}


def get_service_account_key(
    service_account_json: dict | str | None = None,
    service_account_json_path: str | None = None,
) -> dict[str, str] | None:
    """
    Return Yandex Cloud Service Account key loaded from JSON string or file.

    :param service_account_json: Service Account JSON key or dict
    :param service_account_json_path: Service Account JSON key file path
    :return: Yandex Cloud Service Account key
    """
    if service_account_json_path:
        with open(service_account_json_path) as infile:
            service_account_json = infile.read()

    if isinstance(service_account_json, dict):
        return service_account_json
    if service_account_json:
        return json.loads(service_account_json)

    return None


def get_service_account_id(
    service_account_json: dict | str | None = None,
    service_account_json_path: str | None = None,
) -> str | None:
    """
    Return Yandex Cloud Service Account ID loaded from JSON string or file.

    :param service_account_json: Service Account JSON key or dict
    :param service_account_json_path: Service Account JSON key file path
    :return: Yandex Cloud Service Account ID
    """
    sa_key = get_service_account_key(
        service_account_json=service_account_json,
        service_account_json_path=service_account_json_path,
    )
    if sa_key:
        return sa_key.get("service_account_id")
    return None
