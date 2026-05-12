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
from airflow.providers.presto.assets.presto import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("presto://host:8080/hive/default/mytable", id="valid"),
    ],
)
def test_sanitize_uri_pass(value: str) -> None:
    result = sanitize_uri(urllib.parse.urlsplit(value))
    assert result.scheme == "presto"


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("presto:///catalog/schema/table", id="missing-host"),
        pytest.param("presto://host:8080", id="missing-path"),
    ],
)
def test_sanitize_uri_fail(value: str) -> None:
    with pytest.raises(ValueError, match="must contain"):
        sanitize_uri(urllib.parse.urlsplit(value))


@pytest.mark.parametrize(
    ("host", "catalog", "schema", "table", "port", "expected_uri"),
    [
        pytest.param(
            "myhost",
            "hive",
            "default",
            "mytable",
            8080,
            "presto://myhost:8080/hive/default/mytable",
            id="default-port",
        ),
        pytest.param("myhost", "c", "s", "t", 9090, "presto://myhost:9090/c/s/t", id="custom-port"),
    ],
)
def test_create_asset(host: str, catalog: str, schema: str, table: str, port: int, expected_uri: str) -> None:
    result = create_asset(host=host, catalog=catalog, schema=schema, table=table, port=port)
    assert result == Asset(uri=expected_uri)


@pytest.mark.parametrize(
    ("uri", "expected_namespace", "expected_name"),
    [
        pytest.param(
            "presto://myhost:8080/hive/default/mytable",
            "presto://myhost:8080",
            "hive.default.mytable",
            id="default-port",
        ),
        pytest.param(
            "presto://otherhost:9090/postgres/public/users",
            "presto://otherhost:9090",
            "postgres.public.users",
            id="custom-port",
        ),
    ],
)
def test_convert_asset_to_openlineage(uri: str, expected_namespace: str, expected_name: str) -> None:
    asset = Asset(uri=uri)
    result = convert_asset_to_openlineage(asset, None)
    assert result.namespace == expected_namespace
    assert result.name == expected_name
