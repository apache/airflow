#
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

from typing import TypeAlias

from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCatalogCreateAspectTypeOperator,
    DataplexCatalogCreateEntryGroupOperator,
    DataplexCatalogCreateEntryOperator,
    DataplexCatalogCreateEntryTypeOperator,
    DataplexCatalogDeleteAspectTypeOperator,
    DataplexCatalogDeleteEntryGroupOperator,
    DataplexCatalogDeleteEntryOperator,
    DataplexCatalogDeleteEntryTypeOperator,
    DataplexCatalogGetAspectTypeOperator,
    DataplexCatalogGetEntryGroupOperator,
    DataplexCatalogGetEntryOperator,
    DataplexCatalogGetEntryTypeOperator,
    DataplexCatalogListAspectTypesOperator,
    DataplexCatalogListEntriesOperator,
    DataplexCatalogListEntryGroupsOperator,
    DataplexCatalogListEntryTypesOperator,
    DataplexCatalogLookupEntryOperator,
    DataplexCatalogSearchEntriesOperator,
    DataplexCatalogUpdateAspectTypeOperator,
    DataplexCatalogUpdateEntryGroupOperator,
    DataplexCatalogUpdateEntryOperator,
    DataplexCatalogUpdateEntryTypeOperator,
    DataplexCreateAssetOperator,
    DataplexCreateLakeOperator,
    DataplexCreateOrUpdateDataProfileScanOperator,
    DataplexCreateOrUpdateDataQualityScanOperator,
    DataplexCreateTaskOperator,
    DataplexCreateZoneOperator,
    DataplexDeleteAssetOperator,
    DataplexDeleteDataProfileScanOperator,
    DataplexDeleteDataQualityScanOperator,
    DataplexDeleteLakeOperator,
    DataplexDeleteTaskOperator,
    DataplexDeleteZoneOperator,
    DataplexGetDataProfileScanOperator,
    DataplexGetDataProfileScanResultOperator,
    DataplexGetDataQualityScanOperator,
    DataplexGetDataQualityScanResultOperator,
    DataplexGetTaskOperator,
    DataplexListTasksOperator,
    DataplexRunDataProfileScanOperator,
    DataplexRunDataQualityScanOperator,
)

KnowledgeCatalogCreateTaskOperator: TypeAlias = DataplexCreateTaskOperator
KnowledgeCatalogDeleteTaskOperator: TypeAlias = DataplexDeleteTaskOperator
KnowledgeCatalogListTasksOperator: TypeAlias = DataplexListTasksOperator
KnowledgeCatalogGetTaskOperator: TypeAlias = DataplexGetTaskOperator
KnowledgeCatalogCreateLakeOperator: TypeAlias = DataplexCreateLakeOperator
KnowledgeCatalogDeleteLakeOperator: TypeAlias = DataplexDeleteLakeOperator
KnowledgeCatalogCreateOrUpdateDataQualityScanOperator: TypeAlias = (
    DataplexCreateOrUpdateDataQualityScanOperator
)
KnowledgeCatalogGetDataQualityScanOperator: TypeAlias = DataplexGetDataQualityScanOperator
KnowledgeCatalogDeleteDataQualityScanOperator: TypeAlias = DataplexDeleteDataQualityScanOperator
KnowledgeCatalogRunDataQualityScanOperator: TypeAlias = DataplexRunDataQualityScanOperator
KnowledgeCatalogGetDataQualityScanResultOperator: TypeAlias = DataplexGetDataQualityScanResultOperator
KnowledgeCatalogCreateOrUpdateDataProfileScanOperator: TypeAlias = (
    DataplexCreateOrUpdateDataProfileScanOperator
)
KnowledgeCatalogGetDataProfileScanOperator: TypeAlias = DataplexGetDataProfileScanOperator
KnowledgeCatalogDeleteDataProfileScanOperator: TypeAlias = DataplexDeleteDataProfileScanOperator
KnowledgeCatalogRunDataProfileScanOperator: TypeAlias = DataplexRunDataProfileScanOperator
KnowledgeCatalogGetDataProfileScanResultOperator: TypeAlias = DataplexGetDataProfileScanResultOperator
KnowledgeCatalogCreateZoneOperator: TypeAlias = DataplexCreateZoneOperator
KnowledgeCatalogDeleteZoneOperator: TypeAlias = DataplexDeleteZoneOperator
KnowledgeCatalogCreateAssetOperator: TypeAlias = DataplexCreateAssetOperator
KnowledgeCatalogDeleteAssetOperator: TypeAlias = DataplexDeleteAssetOperator
KnowledgeCatalogCreateEntryGroupOperator: TypeAlias = DataplexCatalogCreateEntryGroupOperator
KnowledgeCatalogGetEntryGroupOperator: TypeAlias = DataplexCatalogGetEntryGroupOperator
KnowledgeCatalogDeleteEntryGroupOperator: TypeAlias = DataplexCatalogDeleteEntryGroupOperator
KnowledgeCatalogListEntryGroupsOperator: TypeAlias = DataplexCatalogListEntryGroupsOperator
KnowledgeCatalogUpdateEntryGroupOperator: TypeAlias = DataplexCatalogUpdateEntryGroupOperator
KnowledgeCatalogCreateEntryTypeOperator: TypeAlias = DataplexCatalogCreateEntryTypeOperator
KnowledgeCatalogGetEntryTypeOperator: TypeAlias = DataplexCatalogGetEntryTypeOperator
KnowledgeCatalogDeleteEntryTypeOperator: TypeAlias = DataplexCatalogDeleteEntryTypeOperator
KnowledgeCatalogListEntryTypesOperator: TypeAlias = DataplexCatalogListEntryTypesOperator
KnowledgeCatalogUpdateEntryTypeOperator: TypeAlias = DataplexCatalogUpdateEntryTypeOperator
KnowledgeCatalogCreateAspectTypeOperator: TypeAlias = DataplexCatalogCreateAspectTypeOperator
KnowledgeCatalogGetAspectTypeOperator: TypeAlias = DataplexCatalogGetAspectTypeOperator
KnowledgeCatalogListAspectTypesOperator: TypeAlias = DataplexCatalogListAspectTypesOperator
KnowledgeCatalogUpdateAspectTypeOperator: TypeAlias = DataplexCatalogUpdateAspectTypeOperator
KnowledgeCatalogDeleteAspectTypeOperator: TypeAlias = DataplexCatalogDeleteAspectTypeOperator
KnowledgeCatalogCreateEntryOperator: TypeAlias = DataplexCatalogCreateEntryOperator
KnowledgeCatalogGetEntryOperator: TypeAlias = DataplexCatalogGetEntryOperator
KnowledgeCatalogListEntriesOperator: TypeAlias = DataplexCatalogListEntriesOperator
KnowledgeCatalogSearchEntriesOperator: TypeAlias = DataplexCatalogSearchEntriesOperator
KnowledgeCatalogLookupEntryOperator: TypeAlias = DataplexCatalogLookupEntryOperator
KnowledgeCatalogUpdateEntryOperator: TypeAlias = DataplexCatalogUpdateEntryOperator
KnowledgeCatalogDeleteEntryOperator: TypeAlias = DataplexCatalogDeleteEntryOperator
