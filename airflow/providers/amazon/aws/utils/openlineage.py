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

from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    DocumentationDatasetFacet,
    Fields,
    InputField,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook


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
