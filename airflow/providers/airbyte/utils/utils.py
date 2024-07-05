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


def map_airbyte_type_to_ol(type: str, format: str | None) -> str:
    if type == "string" and format == "date-time":
        return "timestamp"

    if type == "string" and format == "date":
        return "date"

    if type == "string":
        return "string"
    if type == "integer":
        return "int"
    if type == "number":
        return "float"
    if type == "boolean":
        return "boolean"
    if type == "object":
        return "object"
    if type == "array":
        return "array"
    return "string"


def get_field_type(field_metadata: dict[str, Any]) -> str:
    airbyte_field_type = field_metadata.get("type", None)

    airbyte_target_field_type = ""

    if isinstance(airbyte_field_type, list):
        return map_airbyte_types(airbyte_field_type, field_metadata.get("format", None))

    if isinstance(airbyte_field_type, str):
        return map_airbyte_type_to_ol(airbyte_field_type, field_metadata.get("format", None))

    return airbyte_target_field_type


def map_airbyte_types(types: list[str], format: str | None) -> str:
    given_type = filter_non_empty_type(types)
    return map_airbyte_type_to_ol(given_type, format)


def filter_non_empty_type(given_types: list[str]) -> str:
    for given_type in given_types:
        if given_type != "null":
            return given_type

    return ""


def get_streams(connection: dict[str, Any]) -> list[dict[str, Any]]:
    sync_catalog = connection.get("syncCatalog", None)
    if sync_catalog is None:
        return []

    streams = sync_catalog.get("streams", None)
    if streams is None:
        return []

    return streams


def resolve_table_schema(
    name_source: str, source: dict[str, Any], destination: dict[str, Any], connection: dict[str, Any]
) -> str | None:
    """
    Resolve the schema of the table, when data is being synced for airbyte connection.

    User can specify the schema for the destination table, it can be source schema, destination schema or
    custom format schema.

    :param name_source: str enum saying which option has been selected, source, destination or `customformat`
    :param source: str source schema
    :param destination: str destination schema
    :param connection: str custom format schema
    :return:
    """
    if name_source == "source":
        return source.get("namespace", None)

    if name_source == "destination" and destination is not None:
        return destination.get("connectionConfiguration", {}).get("schema", None)

    if name_source == "customformat":
        return connection.get("namespaceFormat", None)

    return None
