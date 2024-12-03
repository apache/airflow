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

if TYPE_CHECKING:
    from airflow.auth.managers.models.resource_details import AssetDetails
    from airflow.sdk.definitions.asset import (
        Asset,
        AssetAlias,
        AssetAliasEvent,
        AssetAll,
        AssetAny,
        expand_alias_to_assets,
    )
else:
    from packaging.version import Version

    from airflow import __version__ as airflow_version

    airflow_base_version = Version(Version(airflow_version).base_version)

    if airflow_base_version >= Version("3.0.0"):
        from airflow.auth.managers.models.resource_details import AssetDetails
        from airflow.sdk.definitions.asset import (
            Asset,
            AssetAlias,
            AssetAliasEvent,
            AssetAll,
            AssetAny,
            expand_alias_to_assets,
        )
    else:
        # dataset is renamed to asset since Airflow 3.0
        from airflow.datasets import Dataset as Asset

        if airflow_base_version >= Version("2.8.0"):
            from airflow.auth.managers.models.resource_details import DatasetDetails as AssetDetails

        if airflow_base_version >= Version("2.9.0"):
            from airflow.datasets import (
                DatasetAll as AssetAll,
                DatasetAny as AssetAny,
            )

        if airflow_base_version >= Version("2.10.0"):
            from airflow.datasets import (
                DatasetAlias as AssetAlias,
                DatasetAliasEvent as AssetAliasEvent,
                expand_alias_to_datasets as expand_alias_to_assets,
            )


__all__ = [
    "Asset",
    "AssetAlias",
    "AssetAliasEvent",
    "AssetAll",
    "AssetAny",
    "AssetDetails",
    "expand_alias_to_assets",
]
