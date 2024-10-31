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

from pydantic import BaseModel, Field, field_validator, model_validator
from typing_extensions import Self

from airflow.utils.log.secrets_masker import redact


# Response Models
class ConnectionResponse(BaseModel):
    """Connection serializer for responses."""

    connection_id: str = Field(serialization_alias="connection_id", validation_alias="conn_id")
    conn_type: str
    description: str | None
    host: str | None
    login: str | None
    schema_: str | None = Field(alias="schema")
    port: int | None
    password: str | None
    extra: str | None

    @model_validator(mode="after")
    def redact_password(self) -> Self:
        if self.password is None:
            return self
        self.password = redact(self.password)
        return self

    @field_validator("extra", mode="before")
    @classmethod
    def redact_extra(cls, v: str | None) -> str | None:
        if v is None:
            return None
        try:
            extra_dict = json.loads(v)
            redacted_dict = redact(extra_dict)
            return json.dumps(redacted_dict)
        except json.JSONDecodeError:
            # we can't redact fields in an unstructured `extra`
            return v


class ConnectionCollectionResponse(BaseModel):
    """Connection Collection serializer for responses."""

    connections: list[ConnectionResponse]
    total_entries: int


# Request Models
class ConnectionBody(BaseModel):
    """Connection Serializer for requests body."""

    connection_id: str = Field(serialization_alias="conn_id")
    conn_type: str
    description: str | None = Field(default=None)
    host: str | None = Field(default=None)
    login: str | None = Field(default=None)
    schema_: str | None = Field(None, alias="schema")
    port: int | None = Field(default=None)
    password: str | None = Field(default=None)
    extra: str | None = Field(default=None)
