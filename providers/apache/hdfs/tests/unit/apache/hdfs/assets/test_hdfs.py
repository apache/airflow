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

from airflow.providers.apache.hdfs.assets.hdfs import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)
from airflow.providers.common.compat.assets import Asset


@pytest.mark.parametrize(
    ("original", "normalized"),
    [
        pytest.param(
            "hdfs://namenode:8020/data/file.csv",
            "hdfs://namenode:8020/data/file.csv",
            id="normalized",
        ),
        pytest.param(
            "hdfs://namenode/data/file.csv",
            "hdfs://namenode/data/file.csv",
            id="no-explicit-port",
        ),
        # ``hdfs:///path`` (Hadoop ``fs.defaultFS``) accepted; ``urlunsplit`` collapses the empty authority to ``hdfs:/path``.
        pytest.param(
            "hdfs:///apps/myapp/data/bronze/raw/table.parquet",
            "hdfs:/apps/myapp/data/bronze/raw/table.parquet",
            id="default-fs-no-host",
        ),
        pytest.param(
            "hdfs:///data/file.csv",
            "hdfs:/data/file.csv",
            id="default-fs-short-path",
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
        pytest.param("hdfs://", id="blank"),
        pytest.param("hdfs://namenode:8020", id="no-path"),
    ],
)
def test_sanitize_uri_fail(value: str) -> None:
    uri_i = urllib.parse.urlsplit(value)
    with pytest.raises(ValueError, match="URI format hdfs:// must contain a path"):
        sanitize_uri(uri_i)


@pytest.mark.parametrize(
    ("path", "expected_uri"),
    [
        pytest.param("/data/file.csv", "hdfs://namenode:8020//data/file.csv", id="root"),
        pytest.param("data/file.csv", "hdfs://namenode:8020/data/file.csv", id="no-leading-slash"),
    ],
)
def test_create_asset(path: str, expected_uri: str) -> None:
    result = create_asset(host="namenode", path=path)
    assert result == Asset(uri=expected_uri)


@pytest.mark.parametrize(
    ("expected_name", "uri"),
    [
        pytest.param("/", "hdfs://namenode:8020", id="no-path"),
        pytest.param("/", "hdfs://namenode:8020/", id="path-slash-only"),
        pytest.param("data/file.csv", "hdfs://namenode:8020//data/file.csv", id="root"),
        pytest.param("data/file.csv", "hdfs://namenode:8020/data/file.csv", id="no-leading-slash"),
        pytest.param("data/file.csv", "hdfs://namenode:8020///data/file.csv", id="two-slashes"),
    ],
)
def test_convert_asset_to_openlineage(expected_name, uri) -> None:
    asset = Asset(uri=uri)
    ol_dataset = convert_asset_to_openlineage(asset=asset, lineage_context=None)
    assert ol_dataset.namespace == "hdfs://namenode:8020"
    assert ol_dataset.name == expected_name


@pytest.mark.parametrize(
    ("expected_name", "uri"),
    [
        pytest.param("apps/myapp/data.parquet", "hdfs:///apps/myapp/data.parquet", id="default-fs"),
        pytest.param("data/file.csv", "hdfs:///data/file.csv", id="default-fs-short"),
    ],
)
def test_convert_asset_to_openlineage_default_fs(expected_name, uri) -> None:
    asset = Asset(uri=uri)
    ol_dataset = convert_asset_to_openlineage(asset=asset, lineage_context=None)
    assert ol_dataset.namespace == "hdfs://"
    assert ol_dataset.name == expected_name
