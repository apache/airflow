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

import logging
import os
import pathlib
from typing import TYPE_CHECKING, Any

from attr import define, field

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Table

    from airflow.providers.common.compat.openlineage.facet import Dataset
    from airflow.utils.context import Context

from airflow.providers.common.compat.openlineage.facet import (
    BaseFacet,
    ColumnLineageDatasetFacet,
    DocumentationDatasetFacet,
    Fields,
    Identifier,
    InputField,
    RunFacet,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
    SymlinksDatasetFacet,
)
from airflow.providers.common.compat.openlineage.utils.spark import (
    inject_parent_job_information_into_spark_properties,
)
from airflow.providers.google import __version__ as provider_version
from airflow.providers.google.cloud.hooks.gcs import _parse_gcs_url

log = logging.getLogger(__name__)

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

    if table.external_data_configuration:
        symlinks = set()
        for uri in table.external_data_configuration.source_uris:
            if uri.startswith("gs://"):
                bucket, blob = _parse_gcs_url(uri)
                blob = extract_ds_name_from_gcs_path(blob)
                symlinks.add((f"gs://{bucket}", blob))

        facets["symlink"] = SymlinksDatasetFacet(
            identifiers=[
                Identifier(namespace=namespace, name=name, type="file")
                for namespace, name in sorted(symlinks)
            ]
        )
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


def _is_openlineage_provider_accessible() -> bool:
    """
    Check if the OpenLineage provider is accessible.

    This function attempts to import the necessary OpenLineage modules and checks if the provider
    is enabled and the listener is available.

    Returns:
        bool: True if the OpenLineage provider is accessible, False otherwise.
    """
    try:
        from airflow.providers.openlineage.conf import is_disabled
        from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
    except ImportError:
        log.debug("OpenLineage provider could not be imported.")
        return False

    if is_disabled():
        log.debug("OpenLineage provider is disabled.")
        return False

    if not get_openlineage_listener():
        log.debug("OpenLineage listener could not be found.")
        return False

    return True


def _extract_supported_job_type_from_dataproc_job(job: dict) -> str | None:
    """
    Extract job type from a Dataproc job definition.

    Args:
        job: The Dataproc job definition.

    Returns:
        The job type for which the automatic OL injection is supported, if found, otherwise None.
    """
    supported_job_types = ("sparkJob", "pysparkJob", "spark_job", "pyspark_job")
    return next((job_type for job_type in supported_job_types if job_type in job), None)


def _replace_dataproc_job_properties(job: dict, job_type: str, new_properties: dict) -> dict:
    """
    Replace the properties of a specific job type in a Dataproc job definition.

    Args:
        job: The original Dataproc job definition.
        job_type: The key representing the job type (e.g., "sparkJob").
        new_properties: The new properties to replace the existing ones.

    Returns:
        A modified copy of the job with updated properties.

    Raises:
        KeyError: If the job_type does not exist in the job or lacks a "properties" field.
    """
    if job_type not in job:
        raise KeyError(f"Job type '{job_type}' is missing in the job definition.")

    updated_job = job.copy()
    updated_job[job_type] = job[job_type].copy()
    updated_job[job_type]["properties"] = new_properties

    return updated_job


def inject_openlineage_properties_into_dataproc_job(
    job: dict, context: Context, inject_parent_job_info: bool
) -> dict:
    """
    Inject OpenLineage properties into Spark job definition.

    Function is not removing any configuration or modifying the job in any other way,
    apart from adding desired OpenLineage properties to Dataproc job definition if not already present.

    Note:
        Any modification to job will be skipped if:
            - OpenLineage provider is not accessible.
            - The job type is not supported.
            - Automatic parent job information injection is disabled.
            - Any OpenLineage properties with parent job information are already present
              in the Spark job definition.

    Args:
        job: The original Dataproc job definition.
        context: The Airflow context in which the job is running.
        inject_parent_job_info: Flag indicating whether to inject parent job information.

    Returns:
        The modified job definition with OpenLineage properties injected, if applicable.
    """
    if not inject_parent_job_info:
        log.debug("Automatic injection of OpenLineage information is disabled.")
        return job

    if not _is_openlineage_provider_accessible():
        log.warning(
            "Could not access OpenLineage provider for automatic OpenLineage "
            "properties injection. No action will be performed."
        )
        return job

    if (job_type := _extract_supported_job_type_from_dataproc_job(job)) is None:
        log.warning(
            "Could not find a supported Dataproc job type for automatic OpenLineage "
            "properties injection. No action will be performed.",
        )
        return job

    properties = job[job_type].get("properties", {})

    properties = inject_parent_job_information_into_spark_properties(properties=properties, context=context)

    job_with_ol_config = _replace_dataproc_job_properties(
        job=job, job_type=job_type, new_properties=properties
    )
    return job_with_ol_config
