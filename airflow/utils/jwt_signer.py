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

from datetime import datetime, timedelta
from typing import Any

import jwt


class JWTSigner:
    """
    Signs and verifies JWT Token. Used to authorise and verify requests.

    :param secret_key: key used to sign the request
    :param expiration_time_in_seconds: time after which the token becomes invalid (in seconds)
    :param audience: audience that the request is expected to have
    :param leeway_in_seconds: leeway that allows for a small clock skew between the two parties
    :param algorithm: algorithm used for signing
    """

    def __init__(
        self,
        secret_key: str,
        expiration_time_in_seconds: int,
        audience: str,
        leeway_in_seconds: int = 5,
        algorithm: str = "HS512",
    ):
        self._secret_key = secret_key
        self._expiration_time_in_seconds = expiration_time_in_seconds
        self._audience = audience
        self._leeway_in_seconds = leeway_in_seconds
        self._algorithm = algorithm

    def generate_signed_token(self, extra_payload: dict[str, Any]) -> str:
        """
        Generate JWT with extra payload added.
        :param extra_payload: extra payload that is added to the signed token
        :return: signed token
        """
        jwt_dict = {
            "aud": self._audience,
            "iat": datetime.utcnow(),
            "nbf": datetime.utcnow(),
            "exp": datetime.utcnow() + timedelta(seconds=self._expiration_time_in_seconds),
        }
        jwt_dict.update(extra_payload)
        token = jwt.encode(
            jwt_dict,
            self._secret_key,
            algorithm=self._algorithm,
        )
        return token

    def verify_token(self, token: str) -> dict[str, Any]:
        payload = jwt.decode(
            token,
            self._secret_key,
            leeway=timedelta(seconds=self._leeway_in_seconds),
            algorithms=[self._algorithm],
            options={
                "verify_signature": True,
                "require": ["exp", "iat", "nbf"],
            },
            audience=self._audience,
        )
        return payload
