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
"""
Publish per-provider versions.json to S3 from deployed directories.

Same pattern as :mod:`airflow_breeze.utils.publish_docs_to_s3` —
the source of truth for which versions exist is what's actually deployed
in S3, not git or a stored manifest.

Usage::

    breeze registry publish-versions --s3-bucket s3://staging-docs-airflow-apache-org/registry/
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import boto3
from packaging.version import InvalidVersion, Version

# Same distribution IDs used by publish_docs_to_s3.py — the registry shares
# the same S3 buckets (and therefore CloudFront distributions) as the docs.
CLOUDFRONT_DISTRIBUTIONS = {
    "live": "E26P75MP9PMULE",
    "staging": "E197MS0XRJC5F3",
}


def parse_s3_url(url: str) -> tuple[str, str]:
    """Parse ``s3://bucket/prefix/`` into (bucket, prefix).

    Examples::

        parse_s3_url("s3://my-bucket/registry/")  -> ("my-bucket", "registry")
        parse_s3_url("s3://my-bucket/")            -> ("my-bucket", "")
        parse_s3_url("s3://my-bucket")             -> ("my-bucket", "")

    When prefix is empty, callers build S3 keys without a leading ``/``
    (e.g. ``providers/{id}`` rather than ``/providers/{id}``).
    """
    path = url.replace("s3://", "")
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
    return bucket, prefix


def list_s3_subdirs(s3, bucket: str, prefix: str) -> list[str]:
    """List immediate subdirectories under an S3 prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    dirs: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"][len(prefix) :].strip("/")
            if name:
                dirs.append(name)
    return dirs


def sort_versions_desc(versions: list[str]) -> list[str]:
    """Sort version strings descending by semver; invalid versions sort last."""

    def key(v: str):
        try:
            return (1, Version(v))
        except InvalidVersion:
            return (0, v)

    return sorted(versions, key=key, reverse=True)


def get_cloudfront_distribution(s3_bucket: str) -> str | None:
    """Return the CloudFront distribution ID for the given S3 bucket URL."""
    if "live-docs" in s3_bucket:
        return CLOUDFRONT_DISTRIBUTIONS["live"]
    if "staging-docs" in s3_bucket:
        return CLOUDFRONT_DISTRIBUTIONS["staging"]
    return None


def invalidate_cloudfront(cloudfront, distribution_id: str) -> None:
    """Invalidate the CloudFront cache for the given distribution."""
    caller_ref = os.environ.get("GITHUB_RUN_ID", "0")
    cloudfront.create_invalidation(
        DistributionId=distribution_id,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": ["/*"]},
            "CallerReference": f"registry-versions-{caller_ref}-{int(time.time())}",
        },
    )
    print(f"  CloudFront cache invalidated (distribution {distribution_id})")


def publish_versions(s3_bucket: str, providers_json_path: Path | None = None) -> None:
    """Publish per-provider versions.json to S3.

    Lists S3 directories under ``providers/{id}/`` to discover deployed
    versions, writes ``api/providers/{id}/versions.json``, then invalidates
    CloudFront.
    """
    if providers_json_path is None:
        providers_json_path = Path("registry/src/_data/providers.json")
        if not providers_json_path.exists():
            providers_json_path = Path("dev/registry/providers.json")

    if not providers_json_path.exists():
        raise FileNotFoundError(f"providers.json not found at {providers_json_path}")

    providers = json.loads(providers_json_path.read_text())["providers"]

    s3 = boto3.client("s3")
    bucket, prefix = parse_s3_url(s3_bucket)

    print(f"Publishing versions.json for {len(providers)} providers")
    print(f"  bucket={bucket}  prefix={prefix}")

    for provider in providers:
        pid = provider["id"]
        latest = provider["version"]
        provider_prefix = f"{prefix}/providers/{pid}" if prefix else f"providers/{pid}"

        deployed = list_s3_subdirs(s3, bucket, provider_prefix)
        versions = sort_versions_desc([v for v in deployed if v and v[0].isdigit()])

        if not versions:
            versions = [latest]

        # If the declared latest isn't deployed yet, use the highest deployed version.
        effective_latest = latest if latest in versions else versions[0]
        if effective_latest not in versions:
            raise ValueError(f"latest version {effective_latest!r} not in versions list for {pid}")
        data = {"versions": versions, "latest": effective_latest}
        key = (
            f"{prefix}/api/providers/{pid}/versions.json" if prefix else f"api/providers/{pid}/versions.json"
        )

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json",
            CacheControl="public, max-age=300",
        )
        print(f"  {pid}: {len(versions)} versions")

    distribution_id = get_cloudfront_distribution(s3_bucket)
    if distribution_id:
        cloudfront = boto3.client("cloudfront")
        invalidate_cloudfront(cloudfront, distribution_id)

    print("Done")
