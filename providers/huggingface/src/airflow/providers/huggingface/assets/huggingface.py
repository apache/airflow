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
"""Airflow asset URI and OpenLineage integration for Hub datasets."""

from __future__ import annotations

from urllib.parse import SplitResult, urlsplit, urlunsplit


def sanitize_uri(uri: SplitResult) -> SplitResult:
    """Normalize dataset URIs and leave other Hub paths unchanged."""
    if uri.scheme != "hf":
        raise ValueError(f"Invalid Hugging Face dataset asset URI: {uri}")
    if uri.netloc != "datasets":
        return uri
    if not uri.path.strip("/"):
        raise ValueError(f"Invalid Hugging Face dataset asset URI: {uri}")
    return uri._replace(scheme="hf", netloc="datasets", path=f"/{uri.path.strip('/')}", query="", fragment="")


def _sanitize_uri_string(uri: str) -> str:
    parsed = urlsplit(uri)
    normalized = sanitize_uri(parsed)
    return uri if normalized == parsed else urlunsplit(normalized)


def create_asset(*, repo_id: str, extra: dict | None = None):
    """Build a dataset asset from the repo ID supplied by Airflow's URI factory."""
    from airflow.sdk import Asset

    return Asset(uri=_sanitize_uri_string(f"hf://datasets/{repo_id}"), extra=extra)


def convert_asset_to_openlineage(asset, lineage_context):
    """Convert an Airflow Hugging Face dataset asset to an OpenLineage dataset."""
    from airflow.providers.common.compat.openlineage.facet import Dataset

    canonical = _sanitize_uri_string(asset.uri)
    return Dataset(namespace="https://huggingface.co", name=canonical.removeprefix("hf://"), facets={})
