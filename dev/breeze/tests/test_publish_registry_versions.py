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
"""Unit tests for airflow_breeze.utils.publish_registry_versions."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.utils.publish_registry_versions import (
    get_cloudfront_distribution,
    invalidate_cloudfront,
    list_s3_subdirs,
    parse_s3_url,
    publish_versions,
    sort_versions_desc,
)


# ---------------------------------------------------------------------------
# parse_s3_url
# ---------------------------------------------------------------------------
class TestParseS3Url:
    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("s3://my-bucket/registry/", ("my-bucket", "registry")),
            ("s3://my-bucket/", ("my-bucket", "")),
            ("s3://my-bucket", ("my-bucket", "")),
            ("s3://bucket/a/b/c/", ("bucket", "a/b/c")),
        ],
        ids=["with-prefix", "trailing-slash-only", "bare-bucket", "nested-prefix"],
    )
    def test_parsing(self, url, expected):
        assert parse_s3_url(url) == expected


# ---------------------------------------------------------------------------
# sort_versions_desc
# ---------------------------------------------------------------------------
class TestSortVersionsDesc:
    def test_semver_ordering(self):
        versions = ["1.0.0", "2.1.0", "1.5.3", "2.0.0"]
        assert sort_versions_desc(versions) == ["2.1.0", "2.0.0", "1.5.3", "1.0.0"]

    def test_single_version(self):
        assert sort_versions_desc(["3.0.0"]) == ["3.0.0"]

    def test_empty_list(self):
        assert sort_versions_desc([]) == []

    def test_invalid_versions_sort_last_descending(self):
        # Invalid versions get key (0, v) which sorts below (1, Version(...))
        # in descending order, ensuring valid semver versions always come first.
        versions = ["2.0.0", "not-a-version", "1.0.0"]
        result = sort_versions_desc(versions)
        assert result[0] == "2.0.0"
        assert result[1] == "1.0.0"
        assert result[2] == "not-a-version"

    def test_pre_release_versions(self):
        versions = ["1.0.0", "1.1.0rc1", "1.1.0"]
        result = sort_versions_desc(versions)
        assert result[0] == "1.1.0"
        assert result[1] == "1.1.0rc1"
        assert result[2] == "1.0.0"


# ---------------------------------------------------------------------------
# list_s3_subdirs
# ---------------------------------------------------------------------------
class TestListS3Subdirs:
    def test_returns_directory_names(self):
        s3 = MagicMock()
        paginator = MagicMock()
        s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "registry/providers/amazon/1.0.0/"},
                    {"Prefix": "registry/providers/amazon/2.0.0/"},
                ]
            }
        ]

        result = list_s3_subdirs(s3, "my-bucket", "registry/providers/amazon")
        assert result == ["1.0.0", "2.0.0"]
        paginator.paginate.assert_called_once_with(
            Bucket="my-bucket",
            Prefix="registry/providers/amazon/",
            Delimiter="/",
        )

    def test_empty_result(self):
        s3 = MagicMock()
        paginator = MagicMock()
        s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{}]

        result = list_s3_subdirs(s3, "my-bucket", "registry/providers/new-provider")
        assert result == []

    def test_multiple_pages(self):
        s3 = MagicMock()
        paginator = MagicMock()
        s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": "pfx/providers/amazon/1.0.0/"}]},
            {"CommonPrefixes": [{"Prefix": "pfx/providers/amazon/2.0.0/"}]},
        ]

        result = list_s3_subdirs(s3, "bucket", "pfx/providers/amazon")
        assert result == ["1.0.0", "2.0.0"]


# ---------------------------------------------------------------------------
# get_cloudfront_distribution
# ---------------------------------------------------------------------------
class TestGetCloudfrontDistribution:
    def test_live_bucket(self):
        assert get_cloudfront_distribution("s3://live-docs-airflow-apache-org/registry/") == "E26P75MP9PMULE"

    def test_staging_bucket(self):
        assert (
            get_cloudfront_distribution("s3://staging-docs-airflow-apache-org/registry/") == "E197MS0XRJC5F3"
        )

    def test_unknown_bucket_returns_none(self):
        assert get_cloudfront_distribution("s3://my-custom-bucket/") is None


# ---------------------------------------------------------------------------
# invalidate_cloudfront
# ---------------------------------------------------------------------------
class TestInvalidateCloudfront:
    def test_creates_invalidation(self, monkeypatch):
        monkeypatch.setenv("GITHUB_RUN_ID", "12345")
        mock_cf = MagicMock()
        invalidate_cloudfront(mock_cf, "E26P75MP9PMULE")
        mock_cf.create_invalidation.assert_called_once()
        batch = mock_cf.create_invalidation.call_args.kwargs["InvalidationBatch"]
        assert batch["Paths"] == {"Quantity": 1, "Items": ["/*"]}
        assert batch["CallerReference"].startswith("registry-versions-12345-")

    def test_default_caller_reference(self):
        mock_cf = MagicMock()
        # No GITHUB_RUN_ID set — falls back to "0"
        with patch.dict("os.environ", {}, clear=True):
            invalidate_cloudfront(mock_cf, "DIST123")
        batch = mock_cf.create_invalidation.call_args.kwargs["InvalidationBatch"]
        assert batch["CallerReference"].startswith("registry-versions-0-")


# ---------------------------------------------------------------------------
# publish_versions (integration with mocked S3)
# ---------------------------------------------------------------------------
class TestPublishVersions:
    def test_publishes_versions_json(self, tmp_path):
        providers_json = tmp_path / "providers.json"
        providers_json.write_text(
            json.dumps(
                {
                    "providers": [
                        {"id": "amazon", "version": "3.0.0"},
                        {"id": "google", "version": "2.0.0"},
                    ]
                }
            )
        )

        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        # amazon has 2 deployed versions, google has none
        def fake_paginate(**kwargs):
            if "amazon" in kwargs["Prefix"]:
                return [
                    {
                        "CommonPrefixes": [
                            {"Prefix": "registry/providers/amazon/2.0.0/"},
                            {"Prefix": "registry/providers/amazon/3.0.0/"},
                        ]
                    }
                ]
            return [{}]

        mock_paginator.paginate.side_effect = fake_paginate

        mock_cloudfront = MagicMock()
        mock_boto3 = MagicMock()

        def fake_client(service):
            if service == "s3":
                return mock_s3
            if service == "cloudfront":
                return mock_cloudfront
            raise ValueError(f"unexpected service: {service}")

        mock_boto3.client.side_effect = fake_client

        # Use a staging bucket URL so CloudFront invalidation is triggered
        with patch("airflow_breeze.utils.publish_registry_versions.boto3", mock_boto3):
            publish_versions(
                "s3://staging-docs-airflow-apache-org/registry/",
                providers_json_path=providers_json,
            )

        # Verify put_object was called for both providers
        put_calls = mock_s3.put_object.call_args_list
        assert len(put_calls) == 2

        # Check amazon versions.json
        amazon_call = next(c for c in put_calls if "amazon" in str(c))
        amazon_body = json.loads(amazon_call.kwargs["Body"])
        assert amazon_body["versions"] == ["3.0.0", "2.0.0"]
        assert amazon_body["latest"] == "3.0.0"
        assert amazon_call.kwargs["Key"] == "registry/api/providers/amazon/versions.json"

        # Check google falls back to latest since no versions deployed
        google_call = next(c for c in put_calls if "google" in str(c))
        google_body = json.loads(google_call.kwargs["Body"])
        assert google_body["versions"] == ["2.0.0"]
        assert google_body["latest"] == "2.0.0"

        # Verify CloudFront invalidation was called
        mock_cloudfront.create_invalidation.assert_called_once()
        inv_call = mock_cloudfront.create_invalidation.call_args
        assert inv_call.kwargs["DistributionId"] == "E197MS0XRJC5F3"

    def test_missing_providers_json_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="nonexistent.json"):
            publish_versions(
                "s3://staging-docs-airflow-apache-org/registry/",
                providers_json_path=tmp_path / "nonexistent.json",
            )
