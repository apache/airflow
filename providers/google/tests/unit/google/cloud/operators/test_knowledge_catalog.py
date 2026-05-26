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

import pytest

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
from airflow.providers.google.cloud.operators.knowledge_catalog import (
    KnowledgeCatalogCreateAspectTypeOperator,
    KnowledgeCatalogCreateAssetOperator,
    KnowledgeCatalogCreateEntryGroupOperator,
    KnowledgeCatalogCreateEntryOperator,
    KnowledgeCatalogCreateEntryTypeOperator,
    KnowledgeCatalogCreateLakeOperator,
    KnowledgeCatalogCreateOrUpdateDataProfileScanOperator,
    KnowledgeCatalogCreateOrUpdateDataQualityScanOperator,
    KnowledgeCatalogCreateTaskOperator,
    KnowledgeCatalogCreateZoneOperator,
    KnowledgeCatalogDeleteAspectTypeOperator,
    KnowledgeCatalogDeleteAssetOperator,
    KnowledgeCatalogDeleteDataProfileScanOperator,
    KnowledgeCatalogDeleteDataQualityScanOperator,
    KnowledgeCatalogDeleteEntryGroupOperator,
    KnowledgeCatalogDeleteEntryOperator,
    KnowledgeCatalogDeleteEntryTypeOperator,
    KnowledgeCatalogDeleteLakeOperator,
    KnowledgeCatalogDeleteTaskOperator,
    KnowledgeCatalogDeleteZoneOperator,
    KnowledgeCatalogGetAspectTypeOperator,
    KnowledgeCatalogGetDataProfileScanOperator,
    KnowledgeCatalogGetDataProfileScanResultOperator,
    KnowledgeCatalogGetDataQualityScanOperator,
    KnowledgeCatalogGetDataQualityScanResultOperator,
    KnowledgeCatalogGetEntryGroupOperator,
    KnowledgeCatalogGetEntryOperator,
    KnowledgeCatalogGetEntryTypeOperator,
    KnowledgeCatalogGetTaskOperator,
    KnowledgeCatalogListAspectTypesOperator,
    KnowledgeCatalogListEntriesOperator,
    KnowledgeCatalogListEntryGroupsOperator,
    KnowledgeCatalogListEntryTypesOperator,
    KnowledgeCatalogListTasksOperator,
    KnowledgeCatalogLookupEntryOperator,
    KnowledgeCatalogRunDataProfileScanOperator,
    KnowledgeCatalogRunDataQualityScanOperator,
    KnowledgeCatalogSearchEntriesOperator,
    KnowledgeCatalogUpdateAspectTypeOperator,
    KnowledgeCatalogUpdateEntryGroupOperator,
    KnowledgeCatalogUpdateEntryOperator,
    KnowledgeCatalogUpdateEntryTypeOperator,
)


@pytest.mark.parametrize(
    ("alias_operator", "original_operator"),
    [
        (KnowledgeCatalogCreateTaskOperator, DataplexCreateTaskOperator),
        (KnowledgeCatalogDeleteTaskOperator, DataplexDeleteTaskOperator),
        (KnowledgeCatalogListTasksOperator, DataplexListTasksOperator),
        (KnowledgeCatalogGetTaskOperator, DataplexGetTaskOperator),
        (KnowledgeCatalogCreateLakeOperator, DataplexCreateLakeOperator),
        (KnowledgeCatalogDeleteLakeOperator, DataplexDeleteLakeOperator),
        (
            KnowledgeCatalogCreateOrUpdateDataQualityScanOperator,
            DataplexCreateOrUpdateDataQualityScanOperator,
        ),
        (KnowledgeCatalogGetDataQualityScanOperator, DataplexGetDataQualityScanOperator),
        (KnowledgeCatalogDeleteDataQualityScanOperator, DataplexDeleteDataQualityScanOperator),
        (KnowledgeCatalogRunDataQualityScanOperator, DataplexRunDataQualityScanOperator),
        (
            KnowledgeCatalogGetDataQualityScanResultOperator,
            DataplexGetDataQualityScanResultOperator,
        ),
        (
            KnowledgeCatalogCreateOrUpdateDataProfileScanOperator,
            DataplexCreateOrUpdateDataProfileScanOperator,
        ),
        (KnowledgeCatalogGetDataProfileScanOperator, DataplexGetDataProfileScanOperator),
        (KnowledgeCatalogDeleteDataProfileScanOperator, DataplexDeleteDataProfileScanOperator),
        (KnowledgeCatalogRunDataProfileScanOperator, DataplexRunDataProfileScanOperator),
        (
            KnowledgeCatalogGetDataProfileScanResultOperator,
            DataplexGetDataProfileScanResultOperator,
        ),
        (KnowledgeCatalogCreateZoneOperator, DataplexCreateZoneOperator),
        (KnowledgeCatalogDeleteZoneOperator, DataplexDeleteZoneOperator),
        (KnowledgeCatalogCreateAssetOperator, DataplexCreateAssetOperator),
        (KnowledgeCatalogDeleteAssetOperator, DataplexDeleteAssetOperator),
        (KnowledgeCatalogCreateEntryGroupOperator, DataplexCatalogCreateEntryGroupOperator),
        (KnowledgeCatalogGetEntryGroupOperator, DataplexCatalogGetEntryGroupOperator),
        (KnowledgeCatalogDeleteEntryGroupOperator, DataplexCatalogDeleteEntryGroupOperator),
        (KnowledgeCatalogListEntryGroupsOperator, DataplexCatalogListEntryGroupsOperator),
        (KnowledgeCatalogUpdateEntryGroupOperator, DataplexCatalogUpdateEntryGroupOperator),
        (KnowledgeCatalogCreateEntryTypeOperator, DataplexCatalogCreateEntryTypeOperator),
        (KnowledgeCatalogGetEntryTypeOperator, DataplexCatalogGetEntryTypeOperator),
        (KnowledgeCatalogDeleteEntryTypeOperator, DataplexCatalogDeleteEntryTypeOperator),
        (KnowledgeCatalogListEntryTypesOperator, DataplexCatalogListEntryTypesOperator),
        (KnowledgeCatalogUpdateEntryTypeOperator, DataplexCatalogUpdateEntryTypeOperator),
        (KnowledgeCatalogCreateAspectTypeOperator, DataplexCatalogCreateAspectTypeOperator),
        (KnowledgeCatalogGetAspectTypeOperator, DataplexCatalogGetAspectTypeOperator),
        (KnowledgeCatalogListAspectTypesOperator, DataplexCatalogListAspectTypesOperator),
        (KnowledgeCatalogUpdateAspectTypeOperator, DataplexCatalogUpdateAspectTypeOperator),
        (KnowledgeCatalogDeleteAspectTypeOperator, DataplexCatalogDeleteAspectTypeOperator),
        (KnowledgeCatalogCreateEntryOperator, DataplexCatalogCreateEntryOperator),
        (KnowledgeCatalogGetEntryOperator, DataplexCatalogGetEntryOperator),
        (KnowledgeCatalogListEntriesOperator, DataplexCatalogListEntriesOperator),
        (KnowledgeCatalogSearchEntriesOperator, DataplexCatalogSearchEntriesOperator),
        (KnowledgeCatalogLookupEntryOperator, DataplexCatalogLookupEntryOperator),
        (KnowledgeCatalogUpdateEntryOperator, DataplexCatalogUpdateEntryOperator),
        (KnowledgeCatalogDeleteEntryOperator, DataplexCatalogDeleteEntryOperator),
    ],
)
def test_knowledge_catalog_aliases(alias_operator, original_operator):
    assert alias_operator is original_operator
