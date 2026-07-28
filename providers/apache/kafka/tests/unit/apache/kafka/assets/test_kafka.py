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

from airflow.providers.apache.kafka.assets.kafka import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)
from airflow.providers.common.compat.assets import Asset


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("kafka://broker1:9092/my-topic", id="valid"),
    ],
)
def test_sanitize_uri_pass(value: str) -> None:
    result = sanitize_uri(urllib.parse.urlsplit(value))
    assert result.scheme == "kafka"
    assert result.netloc == "broker1:9092"


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("kafka:///my-topic", id="missing-host"),
        pytest.param("kafka://broker1:9092", id="missing-topic"),
    ],
)
def test_sanitize_uri_fail(value: str) -> None:
    with pytest.raises(ValueError, match="must contain"):
        sanitize_uri(urllib.parse.urlsplit(value))


def test_create_asset() -> None:
    result = create_asset(server="broker1:9092", topic="my-topic")
    assert result == Asset(uri="kafka://broker1:9092/my-topic")


@pytest.mark.parametrize(
    ("uri", "expected_namespace", "expected_name"),
    [
        pytest.param("kafka://broker1:9092/my-topic", "kafka://broker1:9092", "my-topic", id="basic"),
        pytest.param(
            "kafka://broker1:9092,broker2:9092/events-topic",
            "kafka://broker1:9092,broker2:9092",
            "events-topic",
            id="multiple-brokers",
        ),
    ],
)
def test_convert_asset_to_openlineage(uri: str, expected_namespace: str, expected_name: str) -> None:
    asset = Asset(uri=uri)
    result = convert_asset_to_openlineage(asset, None)
    assert result.namespace == expected_namespace
    assert result.name == expected_name
