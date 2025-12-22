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
"""HBase authenticator factory."""

from __future__ import annotations

from typing import Type

from airflow.providers.hbase.auth.base import (
    HBaseAuthenticator,
    KerberosAuthenticator,
    SimpleAuthenticator,
)


class AuthenticatorFactory:
    """Factory for creating HBase authenticators."""

    _authenticators: dict[str, Type[HBaseAuthenticator]] = {
        "simple": SimpleAuthenticator,
        "kerberos": KerberosAuthenticator,
    }

    @classmethod
    def create(cls, auth_method: str) -> HBaseAuthenticator:
        """
        Create authenticator instance.
        
        :param auth_method: Authentication method name
        :return: Authenticator instance
        """
        if auth_method not in cls._authenticators:
            raise ValueError(
                f"Unknown authentication method: {auth_method}. "
                f"Supported methods: {', '.join(cls._authenticators.keys())}"
            )
        return cls._authenticators[auth_method]()

    @classmethod
    def register(cls, name: str, authenticator_class: Type[HBaseAuthenticator]) -> None:
        """
        Register custom authenticator.
        
        :param name: Authentication method name
        :param authenticator_class: Authenticator class
        """
        cls._authenticators[name] = authenticator_class