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
from typing import TYPE_CHECKING

try:
    from airflow.assets import Asset
except ModuleNotFoundError:
    from airflow.datasets import Dataset as Asset  # type: ignore[no-redef]

if TYPE_CHECKING:
    from urllib.parse import SplitResult

    from airflow.providers.common.compat.openlineage.facet import (
        Dataset as OpenLineageDataset,
    )


def create_asset(*, path: str, extra=None) -> Asset:
    # We assume that we get absolute path starting with /
    return Asset(uri=f"file://{path}", extra=extra)


def sanitize_uri(uri: SplitResult) -> SplitResult:
    if not uri.path:
        raise ValueError("URI format file:// must contain a non-empty path.")
    return uri


def convert_asset_to_openlineage(asset: Asset, lineage_context) -> OpenLineageDataset:
    """
    Translate Asset with valid AIP-60 uri to OpenLineage with assistance from the context.

    Windows paths are not standardized and can produce unexpected behaviour.
    """
    from airflow.providers.common.compat.openlineage.facet import (
        Dataset as OpenLineageDataset,
    )

    parsed = urllib.parse.urlsplit(asset.uri)
    return OpenLineageDataset(namespace=f"file://{parsed.netloc}", name=parsed.path)
