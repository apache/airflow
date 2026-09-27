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

import json
from pathlib import Path

import jsonschema
import pytest
import yaml

import airflow

SCHEMA_PATH = Path(airflow.__file__).parent / "provider.yaml.schema.json"

# A connection type is registered under this exact string, so the authoring schema constrains
# it to the one spelling that can be reached again: get_uri() lowercases and encodes '_' as
# '-', and reading a connection back decodes '-' to '_'.
CONNECTION_TYPE_SCHEMA = json.loads(SCHEMA_PATH.read_text())["properties"]["connection-types"]["items"][
    "properties"
]["connection-type"]

# A YAML block scalar keeps a trailing newline, which is the value most easily written by
# accident. It is built here through the parser rather than hard-coded, so the case documents
# how such a value reaches the schema at all.
BLOCK_SCALAR_VALUE = yaml.safe_load("connection-type: |\n  pydanticai_vertex\n")["connection-type"]


@pytest.mark.parametrize(
    "connection_type",
    ["pydanticai", "google_cloud_platform", "pagerduty_events", "a", "s3"],
)
def test_schema_accepts_a_registrable_connection_type(connection_type):
    jsonschema.validate(connection_type, schema=CONNECTION_TYPE_SCHEMA)


@pytest.mark.parametrize(
    ("connection_type", "reason"),
    [
        pytest.param("pydanticai-vertex", "'-' is the URI encoding of '_'", id="hyphen"),
        pytest.param(BLOCK_SCALAR_VALUE, "a trailing newline is not part of the name", id="block-scalar"),
        pytest.param("pydanticai-vertex\n", "hyphen and trailing newline", id="hyphen-and-newline"),
        pytest.param("PydanticAI", "get_uri() lowercases the scheme", id="uppercase"),
        pytest.param("1password", "a URI scheme cannot start with a digit", id="leading-digit"),
        pytest.param("pydantic ai", "a space cannot appear in a scheme", id="space"),
        pytest.param("pydantic\nai", "an embedded newline is not a scheme", id="embedded-newline"),
        pytest.param("", "a connection type is required to be non-empty", id="empty"),
    ],
)
def test_schema_rejects_a_connection_type_that_cannot_be_resolved(connection_type, reason):
    """
    The trailing-newline cases are why the pattern carries a lookahead rather than ending at
    '$'. A schema pattern is matched with ``re.search``, and Python's '$' matches before a
    trailing newline as well as at the end of the string, so ``^[a-z][a-z0-9_]*$`` alone
    accepts a value written as a YAML block scalar.
    """
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(connection_type, schema=CONNECTION_TYPE_SCHEMA)
