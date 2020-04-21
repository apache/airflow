# -*- coding: utf-8 -*-
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
"""
PostgreSQL to GCS operator.
"""

import datetime
import json
import time
from decimal import Decimal

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.sql_to_gcs import BaseSQLToGoogleCloudStorageOperator
from airflow.utils.decorators import apply_defaults


class PostgresToGoogleCloudStorageOperator(BaseSQLToGoogleCloudStorageOperator):
    """
    Copy data from Postgres to Google Cloud Storage in JSON or CSV format.

    :param postgres_conn_id: Reference to a specific Postgres hook.
    :type postgres_conn_id: str
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
    def __init__(self,
                 postgres_conn_id='postgres_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def query(self):
        """
        Queries Postgres and returns a cursor to the results.
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql, self.parameters)
        return cursor

    def field_to_bigquery(self, field):
        return {
            'name': field[0],
            'type': self.type_map.get(field[1], "STRING"),
            'mode': 'REPEATED' if field[1] in (1009, 1005, 1007,
                                               1016) else 'NULLABLE'
        }

    def convert_type(self, value, schema_type):
        """
        Takes a value from Postgres, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery. Dates are converted to UTC seconds.
        Decimals are converted to floats. Times are converted to seconds.
        """
        if isinstance(value, (datetime.datetime, datetime.date)):
            return time.mktime(value.timetuple())
        if isinstance(value, datetime.time):
            formated_time = time.strptime(str(value), "%H:%M:%S")
            return datetime.timedelta(
                hours=formated_time.tm_hour,
                minutes=formated_time.tm_min,
                seconds=formated_time.tm_sec).seconds
        if isinstance(value, dict):
            return json.dumps(value)
        if isinstance(value, Decimal):
            return float(value)
        return value
