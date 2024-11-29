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

import os
import pathlib
from typing import TYPE_CHECKING, Any

from attr import define, field

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Table

    from airflow.providers.common.compat.openlineage.facet import Dataset

from airflow.providers.common.compat.openlineage.facet import (
    BaseFacet,
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
WILDCARD = "*"


def extract_ds_name_from_gcs_path(path: str) -> str:
    """
    Extract and process the dataset name from a given path.

    Args:
        path: The path to process e.g. of a gcs file.

    Returns:
        The processed dataset name.

    Examples:
        >>> extract_ds_name_from_gcs_path("/dir/file.*")
        'dir'
        >>> extract_ds_name_from_gcs_path("/dir/pre_")
        'dir'
        >>> extract_ds_name_from_gcs_path("/dir/file.txt")
        'dir/file.txt'
        >>> extract_ds_name_from_gcs_path("/dir/file.")
        'dir'
        >>> extract_ds_name_from_gcs_path("/dir/")
        'dir'
        >>> extract_ds_name_from_gcs_path("")
        '/'
        >>> extract_ds_name_from_gcs_path("/")
        '/'
        >>> extract_ds_name_from_gcs_path(".")
        '/'
    """
    if WILDCARD in path:
        path = path.split(WILDCARD, maxsplit=1)[0]

    # We want to end up with parent directory if the path:
    # - does not refer to a file (no dot in the last segment)
    #     and does not explicitly end with a slash, it is treated as a prefix and removed.
    #     Example: "/dir/pre_" -> "/dir/"
    # - contains a dot at the end, then it is treated as a prefix (created after removing the wildcard).
    #     Example: "/dir/file." (was "/dir/file.*" with wildcard) -> "/dir/"
    last_path_segment = os.path.basename(path).rstrip(".")
    if "." not in last_path_segment and not path.endswith("/"):
        path = pathlib.Path(path).parent.as_posix()

    # Normalize the path:
    # - Remove trailing slashes.
    # - Remove leading slashes.
    # - Handle edge cases for empty paths or single-dot paths.
    path = path.rstrip("/")
    path = path.lstrip("/")
    if path in ("", "."):
        path = "/"

    return path


def get_facets_from_bq_table(table: Table) -> dict[str, BaseFacet]:
    """Get facets from BigQuery table object."""
    facets: dict[str, BaseFacet] = {}
    if table.schema:
        facets["schema"] = SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields(
                    name=schema_field.name, type=schema_field.field_type, description=schema_field.description
                )
                for schema_field in table.schema
            ]
        )
    if table.description:
        facets["documentation"] = DocumentationDatasetFacet(description=table.description)

    return facets


def get_identity_column_lineage_facet(
    dest_field_names: list[str],
    input_datasets: list[Dataset],
) -> dict[str, ColumnLineageDatasetFacet]:
    """
    Get column lineage facet for identity transformations.

    This function generates a simple column lineage facet, where each destination column
    consists of source columns of the same name from all input datasets that have that column.
    The lineage assumes there are no transformations applied, meaning the columns retain their
    identity between the source and destination datasets.

    Args:
        dest_field_names: A list of destination column names for which lineage should be determined.
        input_datasets: A list of input datasets with schema facets.

    Returns:
        A dictionary containing a single key, `columnLineage`, mapped to a `ColumnLineageDatasetFacet`.
         If no column lineage can be determined, an empty dictionary is returned - see Notes below.

    Notes:
        - If any input dataset lacks a schema facet, the function immediately returns an empty dictionary.
        - If any field in the source dataset's schema is not present in the destination table,
          the function returns an empty dictionary. The destination table can contain extra fields, but all
          source columns should be present in the destination table.
        - If none of the destination columns can be matched to input dataset columns, an empty
          dictionary is returned.
        - Extra columns in the destination table that do not exist in the input datasets are ignored and
          skipped in the lineage facet, as they cannot be traced back to a source column.
        - The function assumes there are no transformations applied, meaning the columns retain their
          identity between the source and destination datasets.
    """
    fields_sources: dict[str, list[Dataset]] = {}
    for ds in input_datasets:
        if not ds.facets or "schema" not in ds.facets:
            return {}
        for schema_field in ds.facets["schema"].fields:  # type: ignore[attr-defined]
            if schema_field.name not in dest_field_names:
                return {}
            fields_sources[schema_field.name] = fields_sources.get(schema_field.name, []) + [ds]

    if not fields_sources:
        return {}

    column_lineage_facet = ColumnLineageDatasetFacet(
        fields={
            field_name: Fields(
                inputFields=[
                    InputField(namespace=dataset.namespace, name=dataset.name, field=field_name)
                    for dataset in source_datasets
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            )
            for field_name, source_datasets in fields_sources.items()
        }
    )
    return {"columnLineage": column_lineage_facet}


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
