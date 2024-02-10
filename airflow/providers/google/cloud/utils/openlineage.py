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
"""This module contains code related to OpenLineage and lineage extraction."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from openlineage.client.facet import (
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    DocumentationDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
)

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Table
    from openlineage.client.run import Dataset


def get_facets_from_bq_table(table: Table) -> dict[Any, Any]:
    """Get facets from BigQuery table object."""
    facets = {
        "schema": SchemaDatasetFacet(
            fields=[
                SchemaField(name=field.name, type=field.field_type, description=field.description)
                for field in table.schema
            ]
        ),
        "documentation": DocumentationDatasetFacet(description=table.description or ""),
    }

    return facets


def get_identity_column_lineage_facet(
    field_names: list[str],
    input_datasets: list[Dataset],
) -> ColumnLineageDatasetFacet:
    """
    Get column lineage facet.

    Simple lineage will be created, where each source column corresponds to single destination column
    in each input dataset and there are no transformations made.
    """
    if field_names and not input_datasets:
        raise ValueError("When providing `field_names` You must provide at least one `input_dataset`.")

    column_lineage_facet = ColumnLineageDatasetFacet(
        fields={
            field: ColumnLineageDatasetFacetFieldsAdditional(
                inputFields=[
                    ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                        namespace=dataset.namespace, name=dataset.name, field=field
                    )
                    for dataset in input_datasets
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            )
            for field in field_names
        }
    )
    return column_lineage_facet
