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
MySQL to GCS operator.
"""

import base64
import calendar
from datetime import date, datetime, timedelta
from decimal import Decimal

from MySQLdb.constants import FIELD_TYPE

from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.decorators import apply_defaults
from airflow.operators.sql_to_gcs import BaseSQLToGoogleCloudStorageOperator


class MySqlToGoogleCloudStorageOperator(BaseSQLToGoogleCloudStorageOperator):
    """Copy data from MySQL to Google cloud storage in JSON or CSV format.

    :param mysql_conn_id: Reference to a specific MySQL hook.
    :type mysql_conn_id: str
    :param ensure_utc: Ensure TIMESTAMP columns exported as UTC. If set to
        `False`, TIMESTAMP columns will be exported using the MySQL server's
        default timezone.
    :type ensure_utc: bool
    """
    ui_color = '#a0e08c'

    type_map = {
        FIELD_TYPE.BIT: 'INTEGER',
        FIELD_TYPE.DATETIME: 'TIMESTAMP',
        FIELD_TYPE.DATE: 'TIMESTAMP',
        FIELD_TYPE.DECIMAL: 'FLOAT',
        FIELD_TYPE.NEWDECIMAL: 'FLOAT',
        FIELD_TYPE.DOUBLE: 'FLOAT',
        FIELD_TYPE.FLOAT: 'FLOAT',
        FIELD_TYPE.INT24: 'INTEGER',
        FIELD_TYPE.LONG: 'INTEGER',
        FIELD_TYPE.LONGLONG: 'INTEGER',
        FIELD_TYPE.SHORT: 'INTEGER',
        FIELD_TYPE.TIME: 'TIME',
        FIELD_TYPE.TIMESTAMP: 'TIMESTAMP',
        FIELD_TYPE.TINY: 'INTEGER',
        FIELD_TYPE.YEAR: 'INTEGER',
    }

    @apply_defaults
    def __init__(self,
                 mysql_conn_id='mysql_default',
                 ensure_utc=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.ensure_utc = ensure_utc

    def query(self):
        """
        Queries mysql and returns a cursor to the results.
        """
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = mysql.get_conn()
        cursor = conn.cursor()
        if self.ensure_utc:
            # Ensure TIMESTAMP results are in UTC
            tz_query = "SET time_zone = '+00:00'"
            self.log.info('Executing: %s', tz_query)
            cursor.execute(tz_query)
        self.log.info('Executing: %s', self.sql)
        cursor.execute(self.sql)
        return cursor

    def field_to_bigquery(self, field):
        field_type = self.type_map.get(field[1], "STRING")
        # Always allow TIMESTAMP to be nullable. MySQLdb returns None types
        # for required fields because some MySQL timestamps can't be
        # represented by Python's datetime (e.g. 0000-00-00 00:00:00).
        field_mode = "NULLABLE" if field[6] or field_type == "TIMESTAMP" else "REQUIRED"
        return {
            'name': field[0],
            'type': field_type,
            'mode': field_mode,
        }

    def convert_type(self, value, schema_type):
        """
        Takes a value from MySQLdb, and converts it to a value that's safe for
        JSON/Google cloud storage/BigQuery. Dates are converted to UTC seconds.
        Decimals are converted to floats. Binary type fields are encoded with base64,
        as imported BYTES data must be base64-encoded according to Bigquery SQL
        date type documentation: https://cloud.google.com/bigquery/data-types

        :param value: MySQLdb column value
        :type value: Any
        :param schema_type: BigQuery data type
        :type schema_type: str
        """
        if isinstance(value, (datetime, date)):
            return calendar.timegm(value.timetuple())
        if isinstance(value, timedelta):
            return value.total_seconds()
        if isinstance(value, Decimal):
            return float(value)
        if schema_type == "BYTES":
            return base64.standard_b64encode(value).decode('ascii')
        return value
