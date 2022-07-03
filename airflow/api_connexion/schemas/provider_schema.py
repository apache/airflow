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

from typing import NamedTuple

from marshmallow import Schema, fields

from airflow.typing_compat import TypedDict


class ProviderSchema(Schema):
    """Provider schema."""

    package_name = fields.String(required=True)
    description = fields.String(required=True)
    version = fields.String(required=True)


class Provider(TypedDict):
    """A provider."""

    package_name: str
    description: str
    version: str


class ProviderCollection(NamedTuple):
    """List of Providers."""

    providers: list[Provider]
    total_entries: int


class ProviderCollectionSchema(Schema):
    """Provider Collection schema."""

    providers = fields.List(fields.Nested(ProviderSchema))
    total_entries = fields.Int()


provider_collection_schema = ProviderCollectionSchema()
provider_schema = ProviderSchema()
