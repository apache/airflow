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

import datetime
import json
import time
import uuid
from decimal import Decimal
from typing import Dict

import pendulum

from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


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
        return self

    def __next__(self):
        if self.rows:
            return self.rows.pop()
        else:
            self.initialized = True
            return next(self.cursor)

    @property
    def description(self):
        """Fetch first row to initialize cursor description when using server side cursor."""
        if not self.initialized:
            element = self.cursor.fetchone()
            self.rows.append(element)
            self.initialized = True
        return self.cursor.description


class PostgresToGCSOperator(BaseSQLToGCSOperator):
    """
    Copy data from Postgres to Google Cloud Storage in JSON or CSV format.

    :param postgres_conn_id: Reference to a specific Postgres hook.
    :type postgres_conn_id: str
    :param use_server_side_cursor: If server-side cursor should be used for querying postgres.
        For detailed info, check https://www.psycopg.org/docs/usage.html#server-side-cursors
    :type use_server_side_cursor: bool
    :param cursor_itersize: How many records are fetched at a time in case of server-side cursor.
    :type cursor_itersize: int
    """

    ui_color = '#a0e08c'

    type_map = {
        1114: 'TIMESTAMP',
        1184: 'TIMESTAMP',
        1082: 'TIMESTAMP',
        1083: 'TIMESTAMP',
        1005: 'INTEGER',
        1007: 'INTEGER',
        1016: 'INTEGER',
        20: 'INTEGER',
        21: 'INTEGER',
        23: 'INTEGER',
        16: 'BOOLEAN',
        700: 'FLOAT',
        701: 'FLOAT',
        1700: 'FLOAT',
    }

    @apply_defaults
    def __init__(
        self,
        *,
        postgres_conn_id='postgres_default',
        use_server_side_cursor=False,
        cursor_itersize=2000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.use_server_side_cursor = use_server_side_cursor
        self.cursor_itersize = cursor_itersize

    def _unique_name(self):
        return f"{self.dag_id}__{self.task_id}__{uuid.uuid4()}" if self.use_server_side_cursor else None

    def query(self):
        """Queries Postgres and returns a cursor to the results."""
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor(name=self._unique_name())
        cursor.execute(self.sql, self.parameters)
        if self.use_server_side_cursor:
            cursor.itersize = self.cursor_itersize
            return _PostgresServerSideCursorDecorator(cursor)
        return cursor

    def field_to_bigquery(self, field) -> Dict[str, str]:
        return {
            'name': field[0],
            'type': self.type_map.get(field[1], "STRING"),
            'mode': 'REPEATED' if field[1] in (1009, 1005, 1007, 1016) else 'NULLABLE',
        }

    def convert_type(self, value, schema_type):
        """
        Takes a value from Postgres, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery. Dates are converted to UTC seconds.
        Decimals are converted to floats. Times are converted to seconds.
        """
        if isinstance(value, (datetime.datetime, datetime.date)):
            return pendulum.parse(value.isoformat()).float_timestamp
        if isinstance(value, datetime.time):
            formatted_time = time.strptime(str(value), "%H:%M:%S")
            return int(
                datetime.timedelta(
                    hours=formatted_time.tm_hour, minutes=formatted_time.tm_min, seconds=formatted_time.tm_sec
                ).total_seconds()
            )
        if isinstance(value, dict):
            return json.dumps(value)
        if isinstance(value, Decimal):
            return float(value)
        return value
