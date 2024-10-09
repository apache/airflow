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

from airflow.providers.trino.assets.trino import sanitize_uri


@pytest.mark.parametrize(
    "original, normalized",
    [
        pytest.param(
            "trino://example.com:1234/catalog/schema/table",
            "trino://example.com:1234/catalog/schema/table",
            id="normalized",
        ),
        pytest.param(
            "trino://example.com/catalog/schema/table",
            "trino://example.com:8080/catalog/schema/table",
            id="default-port",
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
        pytest.param("trino://", id="blank"),
        pytest.param("trino:///catalog/schema/table", id="no-host"),
        pytest.param("trino://example.com/catalog/table", id="missing-component"),
        pytest.param("trino://example.com:abcd/catalog/schema/table", id="non-port"),
        pytest.param("trino://example.com/catalog/schema/table/column", id="extra-component"),
    ],
)
def test_sanitize_uri_fail(value: str) -> None:
    uri_i = urllib.parse.urlsplit(value)
    with pytest.raises(ValueError):
        sanitize_uri(uri_i)
