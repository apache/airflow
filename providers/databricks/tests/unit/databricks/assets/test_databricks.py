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
from airflow.providers.databricks.assets.databricks import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)


@pytest.mark.parametrize(
    ("original", "normalized"),
    [
        pytest.param(
            "databricks://my-workspace.cloud.databricks.com/catalog/schema/table",
            "databricks://my-workspace.cloud.databricks.com/catalog/schema/table",
            id="normalized",
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
        pytest.param("databricks://", id="blank"),
        pytest.param("databricks:///catalog/schema/table", id="no-host"),
        pytest.param("databricks://host/catalog/table", id="missing-component"),
        pytest.param("databricks://host/catalog/schema/table/column", id="extra-component"),
    ],
)
def test_sanitize_uri_fail(value: str) -> None:
    uri_i = urllib.parse.urlsplit(value)
    with pytest.raises(ValueError, match="URI format databricks:// must contain"):
        sanitize_uri(uri_i)


def test_create_asset() -> None:
    result = create_asset(
        host="my-workspace.cloud.databricks.com",
        catalog="main",
        schema="default",
        table="users",
    )
    assert result == Asset(uri="databricks://my-workspace.cloud.databricks.com/main/default/users")


def test_convert_asset_to_openlineage() -> None:
    asset = Asset(uri="databricks://my-workspace.cloud.databricks.com/main/default/users")
    ol_dataset = convert_asset_to_openlineage(asset=asset, lineage_context=None)
    assert ol_dataset.namespace == "databricks://my-workspace.cloud.databricks.com"
    assert ol_dataset.name == "main.default.users"
