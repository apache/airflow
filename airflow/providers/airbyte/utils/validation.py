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

from jsonschema import validate
from jsonschema.exceptions import ValidationError

STREAM_JSON_DATA_SCHEMA_PROPERTIES = {
    "type": "object",
}

STREAM_JSON_DATA_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string"},
        "properties": STREAM_JSON_DATA_SCHEMA_PROPERTIES,
    },
    "required": ["properties"],
}

STREAM_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "jsonSchema": STREAM_JSON_DATA_SCHEMA,
    },
    "required": ["name", "jsonSchema"],
}

SYNC_CATALOG = {
    "type": "object",
    "properties": {
        "streams": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"stream": STREAM_SCHEMA},
                "required": ["stream"],
            },
        },
    },
    "required": ["streams"],
}

CONNECTION_SCHEMA = {
    "type": "object",
    "properties": {
        "destinationId": {"type": "string"},
        "prefix": {"type": "string"},
        "namespaceDefinition": {"type": "string"},
        "syncCatalog": SYNC_CATALOG,
    },
    "required": ["destinationId", "namespaceDefinition", "syncCatalog"],
}


def is_connection_valid(connection: dict[str, Any]) -> bool:
    try:
        validate(connection, CONNECTION_SCHEMA)
    except ValidationError:
        return False

    return True
