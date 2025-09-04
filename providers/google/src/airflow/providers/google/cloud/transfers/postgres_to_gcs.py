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
"""PostgreSQL to GCS operator."""

from __future__ import annotations

import datetime
import json
import time
import uuid
from decimal import Decimal
from functools import cached_property
from typing import TYPE_CHECKING

import pendulum
from slugify import slugify

from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.postgres.hooks.postgres import USE_PSYCOPG3, PostgresHook

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage


class _PostgresServerSideCursorDecorator:
    """
    Inspired by `_PrestoToGCSPrestoCursorAdapter` to keep this consistent.

    Decorator for allowing description to be available for postgres cursor in case server side
    cursor is used. It doesn't provide other methods except those needed in BaseSQLToGCSOperator,
    which is more of a safety feature.
    """

    def __init__(self, cursor):
        self.cursor = cursor
        self.rows = []
        self.initialized = False

    def __iter__(self):
        """Make the cursor iterable."""
        return self

    def __next__(self):
        """Fetch next row from the cursor."""
        if USE_PSYCOPG3:
            if self.rows:
                return self.rows.pop()
            self.initialized = True
            row = self.cursor.fetchone()
            if row is None:
                raise StopIteration
            return row
        # psycopg2
        if self.rows:
            return self.rows.pop()
        self.initialized = True
        return next(self.cursor)

    @property
    def description(self):
        """Fetch first row to initialize cursor description when using server side cursor."""
        if not self.initialized:
            element = self.cursor.fetchone()
            if element is not None:
                self.rows.append(element)
            self.initialized = True
        return self.cursor.description


class PostgresToGCSOperator(BaseSQLToGCSOperator):
    """
    Copy data from Postgres to Google Cloud Storage in JSON, CSV or Parquet format.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PostgresToGCSOperator`

    :param postgres_conn_id: Reference to a specific Postgres hook.
    :param use_server_side_cursor: If server-side cursor should be used for querying postgres.
        For detailed info, check https://www.psycopg.org/docs/usage.html#server-side-cursors
    :param cursor_itersize: How many records are fetched at a time in case of server-side cursor.
    """

    ui_color = "#a0e08c"

    type_map = {
        1114: "DATETIME",
        1184: "TIMESTAMP",
        1082: "DATE",
        1083: "TIME",
        1005: "INTEGER",
        1007: "INTEGER",
        1016: "INTEGER",
        20: "INTEGER",
        21: "INTEGER",
        23: "INTEGER",
        16: "BOOL",
        700: "FLOAT",
        701: "FLOAT",
        1700: "FLOAT",
    }

    def __init__(
        self,
        *,
        postgres_conn_id="postgres_default",
        use_server_side_cursor=False,
        cursor_itersize=2000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.use_server_side_cursor = use_server_side_cursor
        self.cursor_itersize = cursor_itersize

    def _unique_name(self):
        """
        Generate a non-deterministic UUID for the cursor name using the task_id and dag_id.

        Ensures the resulting name fits within the maximum length allowed for an identifier in Postgres.
        """
        if self.use_server_side_cursor:
            separator = "__"
            random_sufix = str(uuid.uuid4())
            available_length = 63 - len(random_sufix) - (len(separator) * 2)
            return separator.join(
                (
                    slugify(self.dag_id, allow_unicode=False, max_length=available_length // 2),
                    slugify(self.task_id, allow_unicode=False, max_length=available_length // 2),
                    random_sufix,
                )
            )
        return None

    @cached_property
    def db_hook(self) -> PostgresHook:
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)

    def query(self):
        """Execute the query and return a cursor."""
        conn = self.db_hook.get_conn()

        if USE_PSYCOPG3:
            from psycopg.types.json import register_default_adapters

            # Register JSON handlers for this connection if not already done
            register_default_adapters(conn)

            if self.use_server_side_cursor:
                cursor_name = f"airflow_{self.task_id.replace('-', '_')}_{uuid.uuid4().hex}"[:63]
                cursor = conn.cursor(name=cursor_name)
                cursor.itersize = self.cursor_itersize
                cursor.execute(self.sql, self.parameters)
                return _PostgresServerSideCursorDecorator(cursor)
            cursor = conn.cursor()
            cursor.execute(self.sql, self.parameters)
        else:
            cursor = conn.cursor(name=self._unique_name())
            cursor.execute(self.sql, self.parameters)
            if self.use_server_side_cursor:
                cursor.itersize = self.cursor_itersize
                return _PostgresServerSideCursorDecorator(cursor)
        return cursor

    def field_to_bigquery(self, field) -> dict[str, str]:
        return {
            "name": field[0],
            "type": self.type_map.get(field[1], "STRING"),
            "mode": "REPEATED" if field[1] in (1009, 1005, 1007, 1016) else "NULLABLE",
        }

    def convert_type(self, value, schema_type, stringify_dict=True):
        """
        Take a value from Postgres and convert it to a value safe for JSON/Google Cloud Storage/BigQuery.

        Timezone aware Datetime are converted to UTC seconds.
        Unaware Datetime, Date and Time are converted to ISO formatted strings.
        Decimals are converted to floats.

        :param value: Postgres column value.
        :param schema_type: BigQuery data type.
        :param stringify_dict: Specify whether to convert dict to string.
        """
        if isinstance(value, datetime.datetime):
            iso_format_value = value.isoformat()
            if value.tzinfo is None:
                return iso_format_value
            return pendulum.parse(iso_format_value).float_timestamp
        if isinstance(value, datetime.date):
            return value.isoformat()
        if isinstance(value, datetime.time):
            formatted_time = time.strptime(str(value), "%H:%M:%S")
            time_delta = datetime.timedelta(
                hours=formatted_time.tm_hour, minutes=formatted_time.tm_min, seconds=formatted_time.tm_sec
            )
            return str(time_delta)
        if stringify_dict:
            if USE_PSYCOPG3:
                from psycopg.types.json import Json

                if isinstance(value, (dict, Json)):
                    return json.dumps(value)
            elif isinstance(value, dict):
                return json.dumps(value)
        if isinstance(value, Decimal):
            return float(value)
        return value

    def get_openlineage_facets_on_start(self) -> OperatorLineage | None:
        from airflow.providers.common.compat.openlineage.facet import SQLJobFacet
        from airflow.providers.common.compat.openlineage.utils.sql import get_openlineage_facets_with_sql
        from airflow.providers.openlineage.extractors import OperatorLineage

        sql_parsing_result = get_openlineage_facets_with_sql(
            hook=self.db_hook,
            sql=self.sql,
            conn_id=self.postgres_conn_id,
            database=self.db_hook.database,
        )
        gcs_output_datasets = self._get_openlineage_output_datasets()
        if sql_parsing_result:
            sql_parsing_result.outputs = gcs_output_datasets
            return sql_parsing_result
        return OperatorLineage(outputs=gcs_output_datasets, job_facets={"sql": SQLJobFacet(self.sql)})
