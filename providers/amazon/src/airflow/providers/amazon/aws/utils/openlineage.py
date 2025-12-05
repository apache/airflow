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


def inject_parent_job_information_into_glue_script_args(
    script_args: dict[str, Any], context: Context
) -> dict[str, Any]:
    """
    Inject OpenLineage parent job info into Glue script_args via --conf.

    The parent job information is injected as Spark configuration properties via
    the ``--conf`` argument. This uses the same ``spark.openlineage.*`` properties
    that the OpenLineage Spark integration recognizes.

    Note: We use ``--conf`` instead of ``--customer-driver-env-vars`` because AWS Glue
    requires all environment variable keys to have a ``CUSTOMER_`` prefix, which would
    not be recognized by the OpenLineage Spark integration.

    AWS Glue ``--conf`` format requires multiple properties to be formatted as::

        spark.prop1=val1 --conf spark.prop2=val2 --conf spark.prop3=val3

    See: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html

    Behavior:
        - If OpenLineage provider is not available, returns script_args unchanged
        - If user already set any ``spark.openlineage.parent*`` properties in ``--conf``,
          skips injection to preserve user-provided values
        - Appends to existing ``--conf`` if present (with ``--conf`` prefix)
        - Returns new dict (does not mutate original)

    Args:
        script_args: The Glue job's script_args dict.
        context: Airflow task context.

    Returns:
        Modified script_args with OpenLineage Spark properties injected in ``--conf``.
    """
    info = get_parent_job_information(context)
    if info is None:
        return script_args

    existing_conf = script_args.get("--conf", "")

    # Skip if user already configured OpenLineage parent properties
    if "spark.openlineage.parent" in existing_conf:
        log.debug(
            "OpenLineage parent job properties already present in --conf. "
            "Skipping injection to preserve user-provided values."
        )
        return script_args

    ol_spark_properties = [
        f"spark.openlineage.parentJobNamespace={info.parent_job_namespace}",
        f"spark.openlineage.parentJobName={info.parent_job_name}",
        f"spark.openlineage.parentRunId={info.parent_run_id}",
        f"spark.openlineage.rootParentJobNamespace={info.root_parent_job_namespace}",
        f"spark.openlineage.rootParentJobName={info.root_parent_job_name}",
        f"spark.openlineage.rootParentRunId={info.root_parent_run_id}",
    ]

    # AWS Glue --conf format: "prop1=val1 --conf prop2=val2 --conf prop3=val3"
    # First property has no --conf prefix, subsequent ones do
    new_conf = " --conf ".join(ol_spark_properties)
    if existing_conf:
        new_conf = f"{existing_conf} --conf {new_conf}"

    new_script_args = {**script_args}
    new_script_args["--conf"] = new_conf

    return new_script_args
