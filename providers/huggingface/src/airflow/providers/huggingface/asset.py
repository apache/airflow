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

from urllib.parse import SplitResult, urlsplit


def sanitize_uri(uri: str | SplitResult) -> str | SplitResult:
    """Normalize dataset URIs and leave other Hub paths unchanged."""
    parsed = uri if isinstance(uri, SplitResult) else urlsplit(uri)
    if parsed.scheme != "hf":
        raise ValueError(f"Invalid Hugging Face dataset asset URI: {uri}")
    if parsed.netloc != "datasets":
        return parsed if isinstance(uri, SplitResult) else uri
    if not parsed.path.strip("/"):
        raise ValueError(f"Invalid Hugging Face dataset asset URI: {uri}")
    normalized = parsed._replace(
        scheme="hf", netloc="datasets", path=f"/{parsed.path.strip('/')}", query="", fragment=""
    )
    if isinstance(uri, SplitResult):
        return normalized
    from urllib.parse import urlunsplit
    return urlunsplit(normalized)


def create_asset(*, repo_id: str, extra: dict | None = None):
    """Build a dataset asset from the repo ID supplied by Airflow's URI factory."""
    try:
        from airflow.sdk import Asset
    except ImportError:
        from airflow.datasets import Dataset as Asset
    return Asset(uri=sanitize_uri(f"hf://datasets/{repo_id}"), extra=extra)


def convert_asset_to_openlineage(asset, lineage_context):
    """Convert an Airflow Hugging Face dataset asset to an OpenLineage dataset."""
    from airflow.providers.common.compat.openlineage.facet import Dataset

    canonical = sanitize_uri(asset.uri)
    return Dataset(namespace="https://huggingface.co", name=canonical.removeprefix("hf://"), facets={})
