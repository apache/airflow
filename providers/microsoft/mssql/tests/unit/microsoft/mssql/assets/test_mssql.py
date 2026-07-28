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
from airflow.providers.microsoft.mssql.assets.mssql import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)


@pytest.mark.parametrize(
    ("original", "normalized"),
    [
        pytest.param(
            "mssql://example.com:1234/database/schema/table",
            "mssql://example.com:1234/database/schema/table",
            id="normalized",
        ),
        pytest.param(
            "mssql://example.com/database/schema/table",
            "mssql://example.com:1433/database/schema/table",
            id="default-port",
        ),
        pytest.param(
            "mssql://example.com/instance/database/schema/table",
            "mssql://example.com:1433/instance/database/schema/table",
            id="with-instance-default-port",
        ),
        pytest.param(
            "mssql://my-azure-server.database.windows.net/database/schema/table",
            "mssql://my-azure-server.database.windows.net:1433/database/schema/table",
            id="azure-sql-default-port",
        ),
        pytest.param(
            "mssql://my-azure-server.database.windows.net/instance/database/schema/table",
            "mssql://my-azure-server.database.windows.net:1433/instance/database/schema/table",
            id="azure-sql-with-instance-default-port",
        ),
        pytest.param(
            "mssql://my-fabric-server.my-tenant.fabric.microsoft.com/database/schema/table",
            "mssql://my-fabric-server.my-tenant.fabric.microsoft.com:1433/database/schema/table",
            id="fabric-default-port",
        ),
        pytest.param(
            "mssql://my-fabric-server.my-tenant.fabric.microsoft.com/instance/database/schema/table",
            "mssql://my-fabric-server.my-tenant.fabric.microsoft.com:1433/instance/database/schema/table",
            id="fabric-with-instance-default-port",
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
        pytest.param("mssql://example.com/database", id="missing-component"),
        pytest.param("mssql://example.com/database/schema/table/column/extra", id="extra-component"),
        pytest.param("mssql://my-azure-server.database.windows.net/database", id="azure-missing-component"),
        pytest.param(
            "mssql://my-fabric-server.my-tenant.fabric.microsoft.com/database/schema/table/column/extra",
            id="fabric-extra-component",
        ),
    ],
)
def test_sanitize_uri_fail_invalid_path(value: str) -> None:
    uri_i = urllib.parse.urlsplit(value)
    with pytest.raises(
        ValueError,
        match="URI format mssql:// must contain database, schema, and table/view names with optional instance name",
    ):
        sanitize_uri(uri_i)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("mssql://", id="blank"),
        pytest.param("mssql:///database/schema/table", id="no-host"),
    ],
)
def test_sanitize_uri_fail_missing_host(value: str) -> None:
    uri_i = urllib.parse.urlsplit(value)
    with pytest.raises(ValueError, match="URI format mssql:// must contain a host"):
        sanitize_uri(uri_i)


def test_sanitize_uri_fail_non_port() -> None:
    uri_i = urllib.parse.urlsplit("mssql://example.com:abcd/database/schema/table")
    with pytest.raises(ValueError, match="Port could not be cast to integer value as 'abcd'"):
        sanitize_uri(uri_i)


@pytest.mark.parametrize(
    ("host", "database", "schema", "table", "port", "expected_uri"),
    [
        pytest.param(
            "example.com",
            "mydb",
            "dbo",
            "users",
            1433,
            "mssql://example.com:1433/mydb/dbo/users",
            id="default-port",
        ),
        pytest.param(
            "example.com",
            "mydb",
            "dbo",
            "users",
            1434,
            "mssql://example.com:1434/mydb/dbo/users",
            id="custom-port",
        ),
    ],
)
def test_create_asset(
    host: str, database: str, schema: str, table: str, port: int, expected_uri: str
) -> None:
    result = create_asset(host=host, database=database, schema=schema, table=table, port=port)
    assert result == Asset(uri=expected_uri)


@pytest.mark.parametrize(
    ("uri", "expected_namespace", "expected_name"),
    [
        pytest.param(
            "mssql://example.com:1433/mydb/dbo/users",
            "mssql://example.com:1433",
            "mydb.dbo.users",
            id="default-port",
        ),
        pytest.param(
            "mssql://db-host:1434/testdb/schema1/events",
            "mssql://db-host:1434",
            "testdb.schema1.events",
            id="custom-port",
        ),
        pytest.param(
            "mssql://db-host:1434/sql2019/testdb/schema1/events",
            "mssql://db-host:1434",
            "testdb.schema1.events",
            id="with-instance",
        ),
    ],
)
def test_convert_asset_to_openlineage(uri: str, expected_namespace: str, expected_name: str) -> None:
    asset = Asset(uri=uri)
    ol_dataset = convert_asset_to_openlineage(asset=asset, lineage_context=None)
    assert ol_dataset.namespace == expected_namespace
    assert ol_dataset.name == expected_name
