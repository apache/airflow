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

from typing import TYPE_CHECKING, Any

from attr import define, field

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Table

    from airflow.providers.common.compat.openlineage.facet import Dataset

from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    DocumentationDatasetFacet,
    Fields,
    InputField,
    RunFacet,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from airflow.providers.google import __version__ as provider_version

BIGQUERY_NAMESPACE = "bigquery"
BIGQUERY_URI = "bigquery"


def get_facets_from_bq_table(table: Table) -> dict[Any, Any]:
    """Get facets from BigQuery table object."""
    facets = {
        "schema": SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields(
                    name=field.name, type=field.field_type, description=field.description
                )
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
        raise ValueError(
            "When providing `field_names` You must provide at least one `input_dataset`."
        )

    column_lineage_facet = ColumnLineageDatasetFacet(
        fields={
            field: Fields(
                inputFields=[
                    InputField(
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


@define
class BigQueryJobRunFacet(RunFacet):
    """
    Facet that represents relevant statistics of bigquery run.

    This facet is used to provide statistics about bigquery run.

    :param cached: BigQuery caches query results. Rest of the statistics will not be provided for cached queries.
    :param billedBytes: How many bytes BigQuery bills for.
    :param properties: Full property tree of BigQUery run.
    """

    cached: bool
    billedBytes: int | None = field(default=None)
    properties: str | None = field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://raw.githubusercontent.com/apache/airflow/"
            f"providers-google/{provider_version}/airflow/providers/google/"
            "openlineage/BigQueryJobRunFacet.json"
        )


# TODO: remove BigQueryErrorRunFacet in next release
@define
class BigQueryErrorRunFacet(RunFacet):
    """
    Represents errors that can happen during execution of BigqueryExtractor.

    :param clientError: represents errors originating in bigquery client
    :param parserError: represents errors that happened during parsing SQL provided to bigquery
    """

    clientError: str | None = field(default=None)
    parserError: str | None = field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://raw.githubusercontent.com/apache/airflow/"
            f"providers-google/{provider_version}/airflow/providers/google/"
            "openlineage/BigQueryErrorRunFacet.json"
        )


def get_from_nullable_chain(source: Any, chain: list[str]) -> Any | None:
    """
    Get object from nested structure of objects, where it's not guaranteed that all keys in the nested structure exist.

    Intended to replace chain of `dict.get()` statements.

    Example usage:

    .. code-block:: python

        if (
            not job._properties.get("statistics")
            or not job._properties.get("statistics").get("query")
            or not job._properties.get("statistics").get("query").get("referencedTables")
        ):
            return None
        result = job._properties.get("statistics").get("query").get("referencedTables")

    becomes:

    .. code-block:: python

        result = get_from_nullable_chain(properties, ["statistics", "query", "queryPlan"])
        if not result:
            return None
    """
    # chain.pop modifies passed list, this can be unexpected
    chain = chain.copy()
    chain.reverse()
    try:
        while chain:
            while isinstance(source, list) and len(source) == 1:
                source = source[0]
            next_key = chain.pop()
            if isinstance(source, dict):
                source = source.get(next_key)
            else:
                source = getattr(source, next_key)
        return source
    except AttributeError:
        return None
