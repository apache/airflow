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

from airflow.providers.amazon.aws.assets.redshift import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)
from airflow.providers.common.compat.assets import Asset


@pytest.mark.parametrize(
    ("original", "normalized"),
    [
        pytest.param(
            "redshift://cluster.us-east-1:5439/database/schema/table",
            "redshift://cluster.us-east-1:5439/database/schema/table",
            id="normalized",
        ),
        pytest.param(
            "redshift://cluster.us-east-1/database/schema/table",
            "redshift://cluster.us-east-1:5439/database/schema/table",
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
        pytest.param("redshift://", id="blank"),
        pytest.param("redshift:///database/schema/table", id="no-host"),
        pytest.param("redshift://host/database/table", id="missing-component"),
        pytest.param("redshift://host/database/schema/table/column", id="extra-component"),
    ],
)
def test_sanitize_uri_fail(value: str) -> None:
    uri_i = urllib.parse.urlsplit(value)
    with pytest.raises(ValueError, match="URI format redshift:// must contain"):
        sanitize_uri(uri_i)


def test_sanitize_uri_fail_non_port() -> None:
    uri_i = urllib.parse.urlsplit("redshift://cluster.us-east-1:abcd/database/schema/table")
    with pytest.raises(ValueError, match="Port could not be cast to integer value as 'abcd'"):
        sanitize_uri(uri_i)


def test_create_asset() -> None:
    result = create_asset(host="cluster.us-east-1", database="mydb", schema="public", table="users")
    assert result == Asset(uri="redshift://cluster.us-east-1:5439/mydb/public/users")


def test_create_asset_custom_port() -> None:
    result = create_asset(
        host="cluster.us-east-1", port=5440, database="mydb", schema="public", table="users"
    )
    assert result == Asset(uri="redshift://cluster.us-east-1:5440/mydb/public/users")


def test_convert_asset_to_openlineage() -> None:
    asset = Asset(uri="redshift://cluster.us-east-1:5439/mydb/public/users")
    ol_dataset = convert_asset_to_openlineage(asset=asset, lineage_context=None)
    assert ol_dataset.namespace == "redshift://cluster.us-east-1:5439"
    assert ol_dataset.name == "mydb.public.users"
