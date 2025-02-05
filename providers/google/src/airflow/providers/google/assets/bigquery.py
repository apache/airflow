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

from airflow.providers.common.compat.assets import Asset

if TYPE_CHECKING:
    from urllib.parse import SplitResult

    from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset


def sanitize_uri(uri: SplitResult) -> SplitResult:
    if not uri.netloc:
        raise ValueError("URI format bigquery:// must contain a project ID")
    if len(uri.path.split("/")) != 3:  # Leading slash, database name, and table name.
        raise ValueError("URI format bigquery:// must contain dataset and table names")
    return uri


def create_asset(*, project_id: str, dataset_id: str, table_id: str, extra: dict | None = None) -> Asset:
    return Asset(uri=f"bigquery://{project_id}/{dataset_id}/{table_id}", extra=extra)


def convert_asset_to_openlineage(asset: Asset, lineage_context) -> OpenLineageDataset:
    """Translate Asset with valid AIP-60 uri to OpenLineage with assistance from the hook."""
    from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset

    # Expecting valid URI in format: "bigquery://{project_id}/{dataset_id}/{table_id}"
    project, dataset, table = asset.uri.split("/")[-3:]
    return OpenLineageDataset(namespace="bigquery", name=f"{project}.{dataset}.{table}")
