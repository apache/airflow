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
from unittest.mock import patch
from urllib.parse import urlsplit, urlunsplit

import pytest

from airflow.providers.huggingface.asset import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)


def test_sanitize_dataset_asset_uri_string_and_airflow_split_result():
    uri = "hf://datasets/org/example/"
    assert sanitize_uri(uri) == "hf://datasets/org/example"
    normalized = sanitize_uri(urlsplit(uri))
    assert urlunsplit(normalized) == "hf://datasets/org/example"
    asset = create_asset(repo_id="org/example", extra={"source": "test"})
    assert asset.uri == "hf://datasets/org/example"


@pytest.mark.parametrize("uri", ["hf://org/model", "hf://spaces/org/app"])
def test_non_dataset_hub_asset_uris_pass_through(uri):
    assert sanitize_uri(uri) == uri
    normalized = sanitize_uri(urlsplit(uri))
    assert urlunsplit(normalized) == uri


def test_airflow_asset_factory_and_openlineage_converter_signatures():
    extra = {"owner": "unit-test"}
    asset_kwargs = {"repo_id": "org/example"}
    asset = create_asset(**asset_kwargs, extra=extra)
    assert asset.uri == "hf://datasets/org/example"
    assert asset.extra == extra

    lineage_context = object()
    with patch("airflow.providers.common.compat.openlineage.facet.Dataset") as openlineage_dataset:
        converted = convert_asset_to_openlineage(asset, lineage_context)
    openlineage_dataset.assert_called_once_with(
        namespace="https://huggingface.co", name="datasets/org/example", facets={}
    )
    assert converted is openlineage_dataset.return_value


def test_rejects_malformed_dataset_asset_uri():
    with pytest.raises(ValueError):
        sanitize_uri("hf://datasets/")
