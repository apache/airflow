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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.assets import Asset

if TYPE_CHECKING:
    from urllib.parse import SplitResult

    from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset


def create_dataset(*, bucket: str, key: str, extra=None) -> Asset:
    return Asset(uri=f"s3://{bucket}/{key}", extra=extra)


def sanitize_uri(uri: SplitResult) -> SplitResult:
    if not uri.netloc:
        raise ValueError("URI format s3:// must contain a bucket name")
    return uri


def convert_dataset_to_openlineage(dataset: Asset, lineage_context) -> OpenLineageDataset:
    """Translate Asset with valid AIP-60 uri to OpenLineage with assistance from the hook."""
    from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset

    bucket, key = S3Hook.parse_s3_url(dataset.uri)
    return OpenLineageDataset(namespace=f"s3://{bucket}", name=key if key else "/")
