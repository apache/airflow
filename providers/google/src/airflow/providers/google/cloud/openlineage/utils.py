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

import copy
import logging
import os
import pathlib
import re
from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from google.cloud.dataproc_v1 import Batch, RuntimeConfig

from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    DatasetFacet,
    DocumentationDatasetFacet,
    Fields,
    Identifier,
    InputField,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
    SymlinksDatasetFacet,
)
from airflow.providers.common.compat.openlineage.utils.spark import (
    inject_parent_job_information_into_spark_properties,
    inject_transport_information_into_spark_properties,
)
from airflow.providers.google.cloud.hooks.gcs import _parse_gcs_url

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Table

    from airflow.providers.common.compat.openlineage.facet import Dataset
    from airflow.providers.common.compat.sdk import Context


log = logging.getLogger(__name__)

BIGQUERY_NAMESPACE = "bigquery"
BIGQUERY_URI = "bigquery"
WILDCARD = "*"


def merge_column_lineage_facets(facets: list[ColumnLineageDatasetFacet]) -> ColumnLineageDatasetFacet:
    """
    Merge multiple column lineage facets into a single consolidated facet.

    Specifically, it aggregates input fields and transformations for each field across all provided facets.

    Args:
        facets: Column Lineage Facets to be merged.

    Returns:
        A new Column Lineage Facet containing all fields, their respective input fields and transformations.

    Notes:
        - Input fields are uniquely identified by their `(namespace, name, field)` tuple.
        - If multiple facets contain the same field with the same input field, those input
          fields are merged without duplication.
        - Transformations associated with input fields are also merged. If transformations
          are not supported by the version of the `InputField` class, they will be omitted.
        - Transformation merging relies on a composite key of the field name and input field
          tuple to track and consolidate transformations.

    Examples:
        Case 1: Two facets with the same input field
        ```
        >>> facet1 = ColumnLineageDatasetFacet(
        ...     fields={"columnA": Fields(inputFields=[InputField("namespace1", "dataset1", "field1")])}
        ... )
        >>> facet2 = ColumnLineageDatasetFacet(
        ...     fields={"columnA": Fields(inputFields=[InputField("namespace1", "dataset1", "field1")])}
        ... )
        >>> merged = merge_column_lineage_facets([facet1, facet2])
        >>> merged.fields["columnA"].inputFields
        [InputField("namespace1", "dataset1", "field1")]
        ```

        Case 2: Two facets with different transformations for the same input field
        ```
        >>> facet1 = ColumnLineageDatasetFacet(
        ...     fields={
        ...         "columnA": Fields(
        ...             inputFields=[InputField("namespace1", "dataset1", "field1", transformations=["t1"])]
        ...         )
        ...     }
        ... )
        >>> facet2 = ColumnLineageDatasetFacet(
        ...     fields={
        ...         "columnA": Fields(
        ...             inputFields=[InputField("namespace1", "dataset1", "field1", transformations=["t2"])]
        ...         )
        ...     }
        ... )
        >>> merged = merge_column_lineage_facets([facet1, facet2])
        >>> merged.fields["columnA"].inputFields[0].transformations
        ["t1", "t2"]
        ```
    """
    # Dictionary to collect all unique input fields for each field name
    fields_sources: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    # Dictionary to aggregate transformations for each input field
    transformations: dict[str, list] = defaultdict(list)

    for facet in facets:
        for field_name, single_field in facet.fields.items():
            for input_field in single_field.inputFields:
                input_key_fields = (input_field.namespace, input_field.name, input_field.field)
                fields_sources[field_name].add(input_key_fields)

                if single_transformations := getattr(input_field, "transformations", []):
                    transformation_key = "".join((field_name, *input_key_fields))
                    transformations[transformation_key].extend(single_transformations)

    # Check if the `InputField` class supports the `transformations` attribute (since OL client 1.17.1)
    input_field_allows_transformation_info = True
    try:
        InputField(namespace="a", name="b", field="c", transformations=[])
    except TypeError:
        input_field_allows_transformation_info = False

    return ColumnLineageDatasetFacet(
        fields={
            field_name: Fields(
                inputFields=[
                    InputField(
                        namespace,
                        name,
                        column,
                        transformations.get("".join((field_name, namespace, name, column)), []),
                    )
                    if input_field_allows_transformation_info
                    else InputField(namespace, name, column)
                    for namespace, name, column in sorted(input_fields)
                ],
                transformationType="",  # Legacy transformation information
                transformationDescription="",  # Legacy transformation information
            )
            for field_name, input_fields in fields_sources.items()
        }
    )


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


def get_facets_from_bq_table(table: Table) -> dict[str, DatasetFacet]:
    """Get facets from BigQuery table object."""
    return get_facets_from_bq_table_for_given_fields(table, selected_fields=None)


def get_facets_from_bq_table_for_given_fields(
    table: Table, selected_fields: list[str] | None
) -> dict[str, DatasetFacet]:
    """
    Get facets from BigQuery table object for selected fields only.

    If selected_fields is None, include all fields.
    """
    facets: dict[str, DatasetFacet] = {}
    selected_fields_set = set(selected_fields) if selected_fields else None

    if table.schema:
        facets["schema"] = SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields(
                    name=schema_field.name, type=schema_field.field_type, description=schema_field.description
                )
                for schema_field in table.schema
                if selected_fields_set is None or schema_field.name in selected_fields_set
            ]
        )
    if table.description:
        facets["documentation"] = DocumentationDatasetFacet(description=table.description)

    if table.external_data_configuration:
        symlinks = get_namespace_name_from_source_uris(table.external_data_configuration.source_uris)
        facets["symlink"] = SymlinksDatasetFacet(
            identifiers=[
                Identifier(
                    namespace=namespace, name=name, type="file" if namespace.startswith("gs://") else "table"
                )
                for namespace, name in sorted(symlinks)
            ]
        )
    return facets


def get_namespace_name_from_source_uris(source_uris: Iterable[str]) -> set[tuple[str, str]]:
    result = set()
    for uri in source_uris:
        if uri.startswith("gs://"):
            bucket, blob = _parse_gcs_url(uri)
            result.add((f"gs://{bucket}", extract_ds_name_from_gcs_path(blob)))
        elif uri.startswith("https://googleapis.com/bigtable"):
            regex = r"https://googleapis.com/bigtable/projects/([^/]+)/instances/([^/]+)(?:/appProfiles/([^/]+))?/tables/([^/]+)"
            match = re.match(regex, uri)
            if match:
                project_id, instance_id, table_id = match.groups()[0], match.groups()[1], match.groups()[3]
                result.add((f"bigtable://{project_id}/{instance_id}", table_id))
    return result


def get_identity_column_lineage_facet(
    dest_field_names: Iterable[str],
    input_datasets: Iterable[Dataset],
) -> dict[str, DatasetFacet]:
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
    job: dict, context: Context, inject_parent_job_info: bool, inject_transport_info: bool
) -> dict:
    """
    Inject OpenLineage properties into Spark job definition.

    This function does not remove existing configurations or modify the job definition in any way,
     except to add the required OpenLineage properties if they are not already present.

    The entire properties injection process will be skipped if any condition is met:
        - The OpenLineage provider is not accessible.
        - The job type is unsupported.
        - Both `inject_parent_job_info` and `inject_transport_info` are set to False.

    Additionally, specific information will not be injected if relevant OpenLineage properties already exist.

    Parent job information will not be injected if:
        - Any property prefixed with `spark.openlineage.parent` exists.
        - `inject_parent_job_info` is False.
    Transport information will not be injected if:
        - Any property prefixed with `spark.openlineage.transport` exists.
        - `inject_transport_info` is False.

    Args:
        job: The original Dataproc job definition.
        context: The Airflow context in which the job is running.
        inject_parent_job_info: Flag indicating whether to inject parent job information.
        inject_transport_info: Flag indicating whether to inject transport information.

    Returns:
        The modified job definition with OpenLineage properties injected, if applicable.
    """
    if not inject_parent_job_info and not inject_transport_info:
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

    if inject_parent_job_info:
        log.debug("Injecting OpenLineage parent job information into Spark properties.")
        properties = inject_parent_job_information_into_spark_properties(
            properties=properties, context=context
        )

    if inject_transport_info:
        log.debug("Injecting OpenLineage transport information into Spark properties.")
        properties = inject_transport_information_into_spark_properties(
            properties=properties, context=context
        )

    job_with_ol_config = _replace_dataproc_job_properties(
        job=job, job_type=job_type, new_properties=properties
    )
    return job_with_ol_config


def _is_dataproc_batch_of_supported_type(batch: dict | Batch) -> bool:
    """
    Check if a Dataproc batch is of a supported type for Openlineage automatic injection.

    This function determines if the given batch is of a supported type
    by checking for specific job type attributes or keys in the batch.

    Args:
        batch: The Dataproc batch to check.

    Returns:
        True if the batch is of a supported type (`spark_batch` or
        `pyspark_batch`), otherwise False.
    """
    supported_job_types = ("spark_batch", "pyspark_batch")
    if isinstance(batch, Batch):
        if any(getattr(batch, job_type) for job_type in supported_job_types):
            return True
        return False

    # For dictionary-based batch
    if any(job_type in batch for job_type in supported_job_types):
        return True
    return False


def _extract_dataproc_batch_properties(batch: dict | Batch) -> dict:
    """
    Extract Dataproc batch properties from a Batch object or dictionary.

    This function retrieves the `properties` from the `runtime_config` of a
    Dataproc `Batch` object or a dictionary representation of a batch.

    Args:
        batch: The Dataproc batch to extract properties from.

    Returns:
        Extracted `properties` if found, otherwise an empty dictionary.
    """
    if isinstance(batch, Batch):
        return dict(batch.runtime_config.properties)

    # For dictionary-based batch
    run_time_config = batch.get("runtime_config", {})
    if isinstance(run_time_config, RuntimeConfig):
        return dict(run_time_config.properties)
    return run_time_config.get("properties", {})


def _replace_dataproc_batch_properties(batch: dict | Batch, new_properties: dict) -> dict | Batch:
    """
    Replace the properties of a Dataproc batch.

    Args:
        batch: The original Dataproc batch definition.
        new_properties: The new properties to replace the existing ones.

    Returns:
        A modified copy of the Dataproc batch definition with updated properties.
    """
    batch = copy.deepcopy(batch)
    if isinstance(batch, Batch):
        if not batch.runtime_config:
            batch.runtime_config = RuntimeConfig(properties=new_properties)
        elif isinstance(batch.runtime_config, dict):
            batch.runtime_config["properties"] = new_properties
        else:
            batch.runtime_config.properties = new_properties
        return batch

    # For dictionary-based batch
    run_time_config = batch.get("runtime_config")
    if not run_time_config:
        batch["runtime_config"] = {"properties": new_properties}
    elif isinstance(run_time_config, dict):
        run_time_config["properties"] = new_properties
    else:
        run_time_config.properties = new_properties
    return batch


def inject_openlineage_properties_into_dataproc_batch(
    batch: dict | Batch, context: Context, inject_parent_job_info: bool, inject_transport_info: bool
) -> dict | Batch:
    """
    Inject OpenLineage properties into Dataproc batch definition.

    This function does not remove existing configurations or modify the batch definition in any way,
     except to add the required OpenLineage properties if they are not already present.

    The entire properties injection process will be skipped if any condition is met:
        - The OpenLineage provider is not accessible.
        - The batch type is unsupported.
        - Both `inject_parent_job_info` and `inject_transport_info` are set to False.

    Additionally, specific information will not be injected if relevant OpenLineage properties already exist.

    Parent job information will not be injected if:
        - Any property prefixed with `spark.openlineage.parent` exists.
        - `inject_parent_job_info` is False.
    Transport information will not be injected if:
        - Any property prefixed with `spark.openlineage.transport` exists.
        - `inject_transport_info` is False.

    Args:
        batch: The original Dataproc batch definition.
        context: The Airflow context in which the job is running.
        inject_parent_job_info: Flag indicating whether to inject parent job information.
        inject_transport_info: Flag indicating whether to inject transport information.

    Returns:
        The modified batch definition with OpenLineage properties injected, if applicable.
    """
    if not inject_parent_job_info and not inject_transport_info:
        log.debug("Automatic injection of OpenLineage information is disabled.")
        return batch

    if not _is_openlineage_provider_accessible():
        log.warning(
            "Could not access OpenLineage provider for automatic OpenLineage "
            "properties injection. No action will be performed."
        )
        return batch

    if not _is_dataproc_batch_of_supported_type(batch):
        log.warning(
            "Could not find a supported Dataproc batch type for automatic OpenLineage "
            "properties injection. No action will be performed.",
        )
        return batch

    properties = _extract_dataproc_batch_properties(batch)

    if inject_parent_job_info:
        log.debug("Injecting OpenLineage parent job information into Spark properties.")
        properties = inject_parent_job_information_into_spark_properties(
            properties=properties, context=context
        )

    if inject_transport_info:
        log.debug("Injecting OpenLineage transport information into Spark properties.")
        properties = inject_transport_information_into_spark_properties(
            properties=properties, context=context
        )

    batch_with_ol_config = _replace_dataproc_batch_properties(batch=batch, new_properties=properties)
    return batch_with_ol_config


def inject_openlineage_properties_into_dataproc_workflow_template(
    template: dict, context: Context, inject_parent_job_info: bool, inject_transport_info: bool
) -> dict:
    """
    Inject OpenLineage properties into all Spark jobs within Workflow Template.

    This function does not remove existing configurations or modify the jobs definition in any way,
     except to add the required OpenLineage properties if they are not already present.

    The entire properties injection process for each job will be skipped if any condition is met:
        - The OpenLineage provider is not accessible.
        - The job type is unsupported.
        - Both `inject_parent_job_info` and `inject_transport_info` are set to False.

    Additionally, specific information will not be injected if relevant OpenLineage properties already exist.

    Parent job information will not be injected if:
        - Any property prefixed with `spark.openlineage.parent` exists.
        - `inject_parent_job_info` is False.
    Transport information will not be injected if:
        - Any property prefixed with `spark.openlineage.transport` exists.
        - `inject_transport_info` is False.

    Args:
        template: The original Dataproc Workflow Template definition.
        context: The Airflow context in which the job is running.
        inject_parent_job_info: Flag indicating whether to inject parent job information.
        inject_transport_info: Flag indicating whether to inject transport information.

    Returns:
        The modified Workflow Template definition with OpenLineage properties injected, if applicable.
    """
    if not inject_parent_job_info and not inject_transport_info:
        log.debug("Automatic injection of OpenLineage information is disabled.")
        return template

    if not _is_openlineage_provider_accessible():
        log.warning(
            "Could not access OpenLineage provider for automatic OpenLineage "
            "properties injection. No action will be performed."
        )
        return template

    final_jobs = []
    for single_job_definition in template["jobs"]:
        step_id = single_job_definition["step_id"]
        log.debug("Injecting OpenLineage properties into Workflow step: `%s`", step_id)

        if (job_type := _extract_supported_job_type_from_dataproc_job(single_job_definition)) is None:
            log.debug(
                "Could not find a supported Dataproc job type for automatic OpenLineage "
                "properties injection. No action will be performed.",
            )
            final_jobs.append(single_job_definition)
            continue

        properties = single_job_definition[job_type].get("properties", {})

        if inject_parent_job_info:
            log.debug("Injecting OpenLineage parent job information into Spark properties.")
            properties = inject_parent_job_information_into_spark_properties(
                properties=properties, context=context
            )

        if inject_transport_info:
            log.debug("Injecting OpenLineage transport information into Spark properties.")
            properties = inject_transport_information_into_spark_properties(
                properties=properties, context=context
            )

        job_with_ol_config = _replace_dataproc_job_properties(
            job=single_job_definition, job_type=job_type, new_properties=properties
        )
        final_jobs.append(job_with_ol_config)

    template["jobs"] = final_jobs
    return template
