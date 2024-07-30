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

from airflow.datasets import Dataset
from airflow.providers.amazon.aws.datasets.s3 import (
    convert_dataset_to_openlineage,
    create_dataset,
    sanitize_uri,
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def test_sanitize_uri():
    uri = sanitize_uri(urllib.parse.urlsplit("s3://bucket/dir/file.txt"))
    result = sanitize_uri(uri)
    assert result.scheme == "s3"
    assert result.netloc == "bucket"
    assert result.path == "/dir/file.txt"


def test_sanitize_uri_no_netloc():
    with pytest.raises(ValueError):
        sanitize_uri(urllib.parse.urlsplit("s3://"))


def test_sanitize_uri_no_path():
    uri = sanitize_uri(urllib.parse.urlsplit("s3://bucket"))
    result = sanitize_uri(uri)
    assert result.scheme == "s3"
    assert result.netloc == "bucket"
    assert result.path == ""


def test_create_dataset():
    assert create_dataset(bucket="test-bucket", key="test-path") == Dataset(uri="s3://test-bucket/test-path")
    assert create_dataset(bucket="test-bucket", key="test-dir/test-path") == Dataset(
        uri="s3://test-bucket/test-dir/test-path"
    )


def test_sanitize_uri_trailing_slash():
    uri = sanitize_uri(urllib.parse.urlsplit("s3://bucket/"))
    result = sanitize_uri(uri)
    assert result.scheme == "s3"
    assert result.netloc == "bucket"
    assert result.path == "/"


def test_convert_dataset_to_openlineage_valid():
    uri = "s3://bucket/dir/file.txt"
    ol_dataset = convert_dataset_to_openlineage(dataset=Dataset(uri=uri), lineage_context=S3Hook())
    assert ol_dataset.namespace == "s3://bucket"
    assert ol_dataset.name == "dir/file.txt"


@pytest.mark.parametrize("uri", ("s3://bucket", "s3://bucket/"))
def test_convert_dataset_to_openlineage_no_path(uri):
    ol_dataset = convert_dataset_to_openlineage(dataset=Dataset(uri=uri), lineage_context=S3Hook())
    assert ol_dataset.namespace == "s3://bucket"
    assert ol_dataset.name == "/"
