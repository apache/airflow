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

from airflow.providers.common.compat.assets import Asset
from airflow.providers.mysql.assets.mysql import convert_asset_to_openlineage, create_asset, sanitize_uri


@pytest.mark.parametrize(
    ("original", "normalized"),
    [
        pytest.param(
            "mysql://example.com:1234/database/table",
            "mysql://example.com:1234/database/table",
            id="normalized",
        ),
        pytest.param(
            "mysql://example.com/database/table",
            "mysql://example.com:3306/database/table",
            id="default-port",
        ),
        pytest.param(
            "mariadb://example.com/database/table",
            "mysql://example.com:3306/database/table",
            id="mariadb",
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
        pytest.param("mysql://", id="blank"),
        pytest.param("mysql:///database/table", id="no-host"),
        pytest.param("mysql://example.com/database", id="missing-component"),
        pytest.param("mysql://example.com/database/table/column", id="extra-component"),
    ],
)
def test_sanitize_uri_fail(value: str) -> None:
    uri_i = urllib.parse.urlsplit(value)
    with pytest.raises(ValueError, match="URI format mysql:// must contain"):
        sanitize_uri(uri_i)


def test_sanitize_uri_fail_non_port() -> None:
    uri_i = urllib.parse.urlsplit("mysql://example.com:abcd/database/table")
    with pytest.raises(ValueError, match="Port could not be cast to integer value as 'abcd'"):
        sanitize_uri(uri_i)


@pytest.mark.parametrize(
    ("host", "database", "table", "port", "expected_uri"),
    [
        pytest.param(
            "example.com", "mydb", "users", 3306, "mysql://example.com:3306/mydb/users", id="default-port"
        ),
        pytest.param(
            "example.com", "mydb", "users", 3307, "mysql://example.com:3307/mydb/users", id="custom-port"
        ),
    ],
)
def test_create_asset(host: str, database: str, table: str, port: int, expected_uri: str) -> None:
    result = create_asset(host=host, database=database, table=table, port=port)
    assert result == Asset(uri=expected_uri)


@pytest.mark.parametrize(
    ("uri", "expected_namespace", "expected_name"),
    [
        pytest.param(
            "mysql://example.com:3306/mydb/users", "mysql://example.com:3306", "mydb.users", id="default-port"
        ),
        pytest.param(
            "mysql://db-host:3307/testdb/events", "mysql://db-host:3307", "testdb.events", id="custom-port"
        ),
    ],
)
def test_convert_asset_to_openlineage(uri: str, expected_namespace: str, expected_name: str) -> None:
    asset = Asset(uri=uri)
    ol_dataset = convert_asset_to_openlineage(asset=asset, lineage_context=None)
    assert ol_dataset.namespace == expected_namespace
    assert ol_dataset.name == expected_name
