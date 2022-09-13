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

from typing import Any

import arrow
import jwt
import requests

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable


class PlexusHook(BaseHook):
    """
    Used for jwt token generation and storage to
    make Plexus API calls. Requires email and password
    Airflow variables be created.

    Example:
        - export AIRFLOW_VAR_EMAIL = user@corescientific.com
        - export AIRFLOW_VAR_PASSWORD = *******

    """

    def __init__(self) -> None:
        super().__init__()
        self.__token = None
        self.__token_exp = None
        self.host = "https://apiplexus.corescientific.com/"
        self.user_id = None

    def _generate_token(self) -> Any:
        login = Variable.get("email")
        pwd = Variable.get("password")
        if login is None or pwd is None:
            raise AirflowException("No valid email/password supplied.")
        token_endpoint = self.host + "sso/jwt-token/"
        response = requests.post(token_endpoint, data={"email": login, "password": pwd}, timeout=5)
        if not response.ok:
            raise AirflowException(
                "Could not retrieve JWT Token. "
                f"Status Code: [{response.status_code}]. Reason: {response.reason} - {response.text}"
            )
        token = response.json()["access"]
        payload = jwt.decode(token, verify=False)
        self.user_id = payload["user_id"]
        self.__token_exp = payload["exp"]

        return token

    @property
    def token(self) -> Any:
        """Returns users token"""
        if self.__token is not None:
            if not self.__token_exp or arrow.get(self.__token_exp) <= arrow.now():
                self.__token = self._generate_token()
            return self.__token
        else:
            self.__token = self._generate_token()
            return self.__token
