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

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from pydantic_ai.models import KnownModelName, Model


class ProviderBuilder(Protocol):
    """
    Protocol for creating PydanticAI models from Airflow connection details.

    Implementations of this protocol decide whether they support a given
    connection configuration and how to construct the corresponding model.
    """

    def supports(self, extra: dict[str, Any], api_key: str | None, base_url: str | None) -> bool:
        """
        Check if this builder supports the connection characteristics.

        :param extra: Parsed connection extra JSON.
        :param api_key: Decrypted password/API key from the connection.
        :param base_url: Host/Endpoint from the connection.
        """
        ...

    def build(
        self,
        model_name: str | KnownModelName,
        extra: dict[str, Any],
        api_key: str | None,
        base_url: str | None,
    ) -> Model:
        """
        Construct and return a configured PydanticAI model.

        :param model_name: The requested model identifier.
        :param extra: Parsed connection extra JSON.
        :param api_key: Decrypted password/API key from the connection.
        :param base_url: Host/Endpoint from the connection.
        """
        ...
