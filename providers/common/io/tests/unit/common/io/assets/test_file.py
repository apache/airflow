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

from pathlib import PosixPath
from urllib.parse import urlsplit, urlunsplit

import pytest

from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset
from airflow.providers.common.io.assets.file import (
    Asset,
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)


@pytest.mark.parametrize(
    ("uri", "expected"),
    (
        ("file:///valid/path/", "file:///valid/path/"),
        ("file://C://dir/file", "file://C://dir/file"),
    ),
)
def test_sanitize_uri_valid(uri, expected):
    result = sanitize_uri(urlsplit(uri))
    assert urlunsplit(result) == expected


@pytest.mark.parametrize("uri", ("file://",))
def test_sanitize_uri_invalid(uri):
    with pytest.raises(ValueError, match="URI format file:// must contain a non-empty path."):
        sanitize_uri(urlsplit(uri))


@pytest.mark.parametrize(
    ("path", "uri"),
    (
        ("/asdf/fdsa", "file:///asdf/fdsa"),
        ("file:///asdf/fdsa", "file:///asdf/fdsa"),
        ("file://asdf/fdsa", "file://asdf/fdsa"),
        ("file://127.0.0.1:8080/dir/file.csv", "file://127.0.0.1:8080/dir/file.csv"),
        (PosixPath("file:///tmpdir/some-path.csv"), "file:///tmpdir/some-path.csv"),
        ("file:/tmpdir/some-path.csv", "file:///tmpdir/some-path.csv"),
        ("file:tmpdir/some-path.csv", "file:///tmpdir/some-path.csv"),
    ),
)
def test_file_asset(path, uri):
    assert create_asset(path=path) == Asset(uri=uri)


@pytest.mark.parametrize(
    ("path", "ol_dataset"),
    (
        ("/valid/path", OpenLineageDataset(namespace="file", name="/valid/path")),
        (
            "file://127.0.0.1:8080/dir/file.csv",
            OpenLineageDataset(namespace="file://127.0.0.1:8080", name="/dir/file.csv"),
        ),
        ("file:///C://dir/file", OpenLineageDataset(namespace="file", name="/C://dir/file")),
        ("file://asdf.pdf", OpenLineageDataset(namespace="file", name="/asdf.pdf")),
        ("file:///asdf.pdf", OpenLineageDataset(namespace="file", name="/asdf.pdf")),
        (
            PosixPath("file:///tmp/pytest/test.log"),
            OpenLineageDataset(namespace="file", name="/tmp/pytest/test.log"),
        ),
        ("file:/tmp/pytest/test.log", OpenLineageDataset(namespace="file", name="/tmp/pytest/test.log")),
        ("file:tmp/pytest/test.log", OpenLineageDataset(namespace="file", name="/tmp/pytest/test.log")),
    ),
)
def test_convert_asset_to_openlineage(path, ol_dataset):
    result = convert_asset_to_openlineage(create_asset(path=path), None)
    assert result == ol_dataset
