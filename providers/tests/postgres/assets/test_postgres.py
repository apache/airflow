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

import urllib.parse

import pytest

from airflow.providers.postgres.assets.postgres import sanitize_uri


@pytest.mark.parametrize(
    "original, normalized",
    [
        pytest.param(
            "postgres://example.com:1234/database/schema/table",
            "postgres://example.com:1234/database/schema/table",
            id="normalized",
        ),
        pytest.param(
            "postgres://example.com/database/schema/table",
            "postgres://example.com:5432/database/schema/table",
            id="default-port",
        ),
        pytest.param(
            "postgres://example.com/database//table",
            "postgres://example.com:5432/database/default/table",
            id="default-schema",
        ),
    ],
)
def test_sanitize_uri_pass(original: str, normalized: str) -> None:
    uri_i = urllib.parse.urlsplit(original)
    uri_o = sanitize_uri(uri_i)
    assert urllib.parse.urlunsplit(uri_o) == normalized


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("postgres://", id="blank"),
        pytest.param("postgres:///database/schema/table", id="no-host"),
        pytest.param("postgres://example.com/database/table", id="missing-component"),
        pytest.param("postgres://example.com:abcd/database/schema/table", id="non-port"),
        pytest.param(
            "postgres://example.com/database/schema/table/column", id="extra-component"
        ),
    ],
)
def test_sanitize_uri_fail(value: str) -> None:
    uri_i = urllib.parse.urlsplit(value)
    with pytest.raises(ValueError):
        sanitize_uri(uri_i)
