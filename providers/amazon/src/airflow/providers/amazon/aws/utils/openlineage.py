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
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    DocumentationDatasetFacet,
    Fields,
    InputField,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from airflow.providers.common.compat.openlineage.utils.spark import get_parent_job_information

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
    from airflow.utils.context import Context

log = logging.getLogger(__name__)


def get_facets_from_redshift_table(
    redshift_hook: RedshiftDataHook | RedshiftSQLHook,
    table: str,
    redshift_data_api_kwargs: dict,
    schema: str = "public",
) -> dict[Any, Any]:
    """
    Query redshift for table metadata.

    SchemaDatasetFacet and DocumentationDatasetFacet (if table has description) will be created.
    """
    sql = f"""
    SELECT
        cols.column_name,
        cols.data_type,
        col_des.description as column_description,
        tbl_des.description as table_description
    FROM
        information_schema.columns cols
    LEFT JOIN
        pg_catalog.pg_description col_des
    ON
        cols.ordinal_position = col_des.objsubid
        AND col_des.objoid = (SELECT oid FROM pg_class WHERE relnamespace =
        (SELECT oid FROM pg_namespace WHERE nspname = cols.table_schema) AND relname = cols.table_name)
    LEFT JOIN
        pg_catalog.pg_class tbl
    ON
        tbl.relname = cols.table_name
        AND tbl.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = cols.table_schema)
    LEFT JOIN
        pg_catalog.pg_description tbl_des
    ON
        tbl.oid = tbl_des.objoid
        AND tbl_des.objsubid = 0
    WHERE
        cols.table_name = '{table}'
        AND cols.table_schema = '{schema}';
    """
    if isinstance(redshift_hook, RedshiftSQLHook):
        records = redshift_hook.get_records(sql)
        if records:
            table_description = records[0][-1]  # Assuming the table description is the same for all rows
        else:
            table_description = None
        documentation = DocumentationDatasetFacet(description=table_description or "")
        table_schema = SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields(name=field[0], type=field[1], description=field[2])
                for field in records
            ]
        )
    else:
        statement_id = redshift_hook.execute_query(
            sql=sql, poll_interval=1, **redshift_data_api_kwargs
        ).statement_id
        response = redshift_hook.conn.get_statement_result(Id=statement_id)

        table_schema = SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields(
                    name=field[0]["stringValue"],
                    type=field[1]["stringValue"],
                    description=field[2].get("stringValue"),
                )
                for field in response["Records"]
            ]
        )
        #  Table description will be the same for all fields, so we retrieve it from first field.
        documentation = DocumentationDatasetFacet(
            description=response["Records"][0][3].get("stringValue") or ""
        )

    return {"schema": table_schema, "documentation": documentation}


def get_identity_column_lineage_facet(
    field_names,
    input_datasets,
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
            field: Fields(
                inputFields=[
                    InputField(namespace=dataset.namespace, name=dataset.name, field=field)
                    for dataset in input_datasets
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            )
            for field in field_names
        }
    )
    return column_lineage_facet


def _parse_glue_customer_env_vars(env_vars_string: str | None) -> dict[str, str]:
    """
    Parse the --customer-driver-env-vars format into a dict.

    Format: "KEY1=VAL1,KEY2=\"val2,val2 val2\""
    - Simple values: KEY=VALUE
    - Values with commas/spaces: KEY="value with, spaces"

    Args:
        env_vars_string: The environment variables string from Glue script args.

    Returns:
        Dict of key-value pairs.
    """
    if not env_vars_string:
        return {}

    result: dict[str, str] = {}
    current = ""
    in_quotes = False

    for char in env_vars_string:
        if char == '"' and (not current or current[-1] != "\\"):
            in_quotes = not in_quotes
            current += char
        elif char == "," and not in_quotes:
            if "=" in current:
                key, value = current.split("=", 1)
                # Strip surrounding quotes if present
                value = value.strip()
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                result[key.strip()] = value
            current = ""
        else:
            current += char

    # Handle last element
    if current and "=" in current:
        key, value = current.split("=", 1)
        value = value.strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        result[key.strip()] = value

    return result


def _format_glue_customer_env_vars(env_vars: dict[str, str]) -> str:
    """
    Format a dict back into the --customer-driver-env-vars string format.

    - Values containing commas, spaces, or quotes need quoting
    - Quotes within values need escaping

    Args:
        env_vars: Dict of environment variables.

    Returns:
        String in format "KEY1=VAL1,KEY2=\"val2\""
    """
    parts = []
    for key, value in env_vars.items():
        # Quote if contains special chars
        if "," in value or " " in value or '"' in value:
            escaped_value = value.replace('"', '\\"')
            parts.append(f'{key}="{escaped_value}"')
        else:
            parts.append(f"{key}={value}")
    return ",".join(parts)


def _is_parent_job_info_present_in_glue_env_vars(script_args: dict[str, Any]) -> bool:
    """
    Check if any OpenLineage parent job env vars are already set.

    Args:
        script_args: The Glue job's script_args dict.

    Returns:
        True if any OL parent job env vars are present.
    """
    # Check --customer-driver-env-vars
    driver_env_vars_str = script_args.get("--customer-driver-env-vars", "")
    driver_env_vars = _parse_glue_customer_env_vars(driver_env_vars_str)

    # Also check --customer-executor-env-vars
    executor_env_vars_str = script_args.get("--customer-executor-env-vars", "")
    executor_env_vars = _parse_glue_customer_env_vars(executor_env_vars_str)

    all_env_vars = {**driver_env_vars, **executor_env_vars}

    # Check if ANY OpenLineage parent env var is present
    return any(
        key.startswith("OPENLINEAGE_PARENT") or key.startswith("OPENLINEAGE_ROOT_PARENT")
        for key in all_env_vars
    )


def inject_parent_job_information_into_glue_script_args(
    script_args: dict[str, Any], context: Context
) -> dict[str, Any]:
    """
    Inject OpenLineage parent job info into Glue script_args.

    The parent job information is injected via the --customer-driver-env-vars argument,
    which sets environment variables in the Spark driver process.

    - If OpenLineage provider is not available, skip injection
    - If user already set any OPENLINEAGE_PARENT_* or OPENLINEAGE_ROOT_PARENT_* env vars,
      skip injection to preserve user-provided values
    - Merge with existing --customer-driver-env-vars if present
    - Return new dict (don't mutate original)

    Args:
        script_args: The Glue job's script_args dict.
        context: Airflow task context.

    Returns:
        Modified script_args with OpenLineage env vars injected.
    """
    info = get_parent_job_information(context)
    if info is None:
        return script_args

    existing_env_vars_str = script_args.get("--customer-driver-env-vars", "")
    existing_env_vars = _parse_glue_customer_env_vars(existing_env_vars_str)

    ol_env_vars = {
        "OPENLINEAGE_PARENT_JOB_NAMESPACE": info.parent_job_namespace,
        "OPENLINEAGE_PARENT_JOB_NAME": info.parent_job_name,
        "OPENLINEAGE_PARENT_RUN_ID": info.parent_run_id,
        "OPENLINEAGE_ROOT_PARENT_JOB_NAMESPACE": info.root_parent_job_namespace,
        "OPENLINEAGE_ROOT_PARENT_JOB_NAME": info.root_parent_job_name,
        "OPENLINEAGE_ROOT_PARENT_RUN_ID": info.root_parent_run_id,
    }

    merged_env_vars = {**existing_env_vars, **ol_env_vars}

    new_script_args = {**script_args}
    new_script_args["--customer-driver-env-vars"] = _format_glue_customer_env_vars(merged_env_vars)

    return new_script_args
