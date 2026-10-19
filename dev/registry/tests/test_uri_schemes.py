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
"""Unit tests for dev/registry/registry_tools/uri_schemes.py."""

from __future__ import annotations

import textwrap

import pytest
from registry_contract_models import UriSchemeContract
from registry_tools.uri_schemes import collect_uri_schemes, read_filesystem_schemes

S3_FS_SOURCE = textwrap.dedent("""\
    from __future__ import annotations

    import logging

    S3_PROXY_URI = "proxy-uri"
    log = logging.getLogger(__name__)

    schemes = ["s3", "s3a"]


    def get_fs(conn_id, storage_options=None):
        ...
    """)

S3_ASSET = {
    "handler": "airflow.providers.amazon.aws.assets.s3.sanitize_uri",
    "factory": "airflow.providers.amazon.aws.assets.s3.create_asset",
    "to_openlineage_converter": "airflow.providers.amazon.aws.assets.s3.convert_asset_to_openlineage",
}


class TestReadFilesystemSchemes:
    def test_reads_module_level_schemes_list(self):
        assert read_filesystem_schemes(S3_FS_SOURCE) == ["s3", "s3a"]

    def test_reads_annotated_schemes_list(self):
        assert read_filesystem_schemes('schemes: list[str] = ["gs", "gcs"]\n') == ["gs", "gcs"]

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param("def get_fs():\n    schemes = ['local']\n", id="bound-inside-function"),
            pytest.param('schemes = [*BASE_SCHEMES, "s3"]\n', id="non-literal"),
            pytest.param("schemes = [\n", id="syntax-error"),
        ],
    )
    def test_returns_empty_when_no_literal_module_level_schemes(self, source):
        assert read_filesystem_schemes(source) == []


class TestCollectUriSchemes:
    def test_merges_sections_into_one_entry_per_scheme_sorted(self):
        provider_yaml = {
            "filesystems": ["airflow.providers.amazon.aws.fs.s3"],
            "asset-uris": [{"schemes": ["s3"], **S3_ASSET}],
            "remote-logging": [
                {
                    "classpath": "airflow.providers.amazon.aws.log.s3_task_handler.S3RemoteLogIO",
                    "scheme": "s3",
                },
                {
                    "classpath": "airflow.providers.amazon.aws.log.cloudwatch_task_handler.CloudWatchRemoteLogIO",
                    "scheme": "cloudwatch",
                },
            ],
        }

        result = collect_uri_schemes(provider_yaml, {"airflow.providers.amazon.aws.fs.s3": S3_FS_SOURCE}.get)

        assert result == [
            {
                "scheme": "cloudwatch",
                "remote_logging": "airflow.providers.amazon.aws.log.cloudwatch_task_handler.CloudWatchRemoteLogIO",
            },
            {
                "scheme": "s3",
                "filesystem": "airflow.providers.amazon.aws.fs.s3",
                "asset": S3_ASSET,
                "remote_logging": "airflow.providers.amazon.aws.log.s3_task_handler.S3RemoteLogIO",
            },
            {"scheme": "s3a", "filesystem": "airflow.providers.amazon.aws.fs.s3"},
        ]
        for entry in result:
            UriSchemeContract.model_validate(entry)

    def test_asset_scheme_with_null_handler_is_still_registered(self):
        result = collect_uri_schemes({"asset-uris": [{"schemes": ["gcp"], "handler": None}]}, {}.get)

        assert result == [
            {"scheme": "gcp", "asset": {"handler": None, "factory": None, "to_openlineage_converter": None}}
        ]

    def test_skips_asset_entry_without_handler_key(self):
        result = collect_uri_schemes(
            {
                "asset-uris": [
                    {"schemes": ["s3"], "factory": "airflow.providers.amazon.aws.assets.s3.create_asset"}
                ]
            },
            {}.get,
        )

        assert result == []

    def test_skips_filesystem_module_whose_source_is_missing(self):
        result = collect_uri_schemes({"filesystems": ["airflow.providers.gone.fs.gone"]}, {}.get)

        assert result == []

    def test_provider_without_scheme_sections_returns_empty(self):
        assert collect_uri_schemes({"name": "No Schemes", "hooks": []}, {}.get) == []
