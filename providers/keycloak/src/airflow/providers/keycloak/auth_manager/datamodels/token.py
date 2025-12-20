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
from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, RootModel, model_validator

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel


class TokenResponse(BaseModel):
    """Token serializer for responses."""

    access_token: str


class TokenPasswordBody(StrictBaseModel):
    """Password Grant Token serializer for post bodies."""

    grant_type: Literal["password"] = "password"
    username: str = Field()
    password: str = Field()


class TokenClientCredentialsBody(StrictBaseModel):
    """Client Credentials Grant Token serializer for post bodies."""

    grant_type: Literal["client_credentials"]
    client_id: str = Field()
    client_secret: str = Field()


TokenUnion = Annotated[
    TokenPasswordBody | TokenClientCredentialsBody,
    Field(discriminator="grant_type"),
]


class TokenBody(RootModel[TokenUnion]):
    """Token request body."""

    @model_validator(mode="before")
    @classmethod
    def default_grant_type(cls, data):
        """Add default grant_type for discrimination."""
        if "grant_type" not in data:
            data["grant_type"] = "password"
        return data
