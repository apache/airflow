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
MsSQL to GCS operator.
"""

import decimal

from airflow.operators.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.decorators import apply_defaults


class MSSQLToGCSOperator(BaseSQLToGCSOperator):
    """Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON or CSV format.

    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str

    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::

            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """
    ui_color = '#e0a98c'

    type_map = {
        3: 'INTEGER',
        4: 'TIMESTAMP',
        5: 'NUMERIC'
    }

    @apply_defaults
    def __init__(self,
                 mssql_conn_id='mssql_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id

    def query(self):
        """
        Queries MSSQL and returns a cursor of results.

        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        return cursor

    def field_to_bigquery(self, field):
        return {
            'name': field[0].replace(" ", "_"),
            'type': self.type_map.get(field[1], "STRING"),
            'mode': "NULLABLE",
        }

    @classmethod
    def convert_type(cls, value, schema_type):
        """
        Takes a value from MSSQL, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery.
        """
        if isinstance(value, decimal.Decimal):
            return float(value)
        return value
