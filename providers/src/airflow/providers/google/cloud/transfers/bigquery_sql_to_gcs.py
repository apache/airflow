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
"""This module contains Google BigQuery's sql to Google Cloud Storage operator."""

from datetime import datetime, timezone

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryCursor
from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator

class _BigQueryCursorWrapper:
    """
    Wrapps BigQueryCursor cursor to BaseSQLToGCSOperator's expectations.
    """
    def __init__(self, cursor: BigQueryCursor):
        self.cursor = cursor

    def __iter__(self):
        return self

    # pylint: disable=redefined-builtin
    def __next__(self):
        """
        Wraps standard __next__ to raise StopIteration when over.
        """
        next = self.cursor.next()
        if next is None:
            raise StopIteration
        return next

    @property
    def description(self):
        """
        Forwards description.
        """
        return self.cursor.description

class BigQuerySqlToGCSOperator(BaseSQLToGCSOperator):
    """
    Copy data from Google BigQuery's sql to Google Cloud Storage in JSON, CSV or Parquet format.

    :param use_legacy_sql: This specifies whether to use legacy SQL dialect.
    :param gcp_conn_id: The Airflow connection used for GCP credentials.
    :param override_schema_from_cursor: Flag to allow override schema field
        based on sql result.
    :param custom_value_transform_delegate: The delegate to manually convert
        field into GCS's schema instead of default.
    :param custom_field_to_bigquery_delegate: The delegate to manually convert
        incoming value into desired shape.

    **Example**:
        The following operator will export data from the Customers sql
        into the given google BigQuery Database and then upload it to the
        'bq-sql-export' GCS bucket ::

    .. code-block:: python

        task = BigQuerySqlToGCSOperator(
            task_id="sql_json_raw",
            sql=\"""
                SELECT
                True as _BOOL,
                CODE_POINTS_TO_BYTES([65, 98, 67, 100]) as _BYTES,
                DATE('0001-01-01') as _DATE,
                DATETIME('0001-01-01 00:00:00') as _DATETIME,
                JSON '{"id": 10,"type": "fruit","name": "apple","on_menu": true,"recipes":{"salads":[{ "id": 2001, "type": "Walnut Apple Salad" },{ "id": 2002, "type": "Apple Spinach Salad" }],"desserts":[{ "id": 3001, "type": "Apple Pie" },{ "id": 3002, "type": "Apple Scones" },{ "id": 3003, "type": "Apple Crumble" }]}}' as _JSON,
                -9223372036854775808 as _INT64,
                cast(-9.9999999999999999999999999999999999999E+28 as NUMERIC) as _NUMERIC,
                cast(-5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38 as BIGNUMERIC) as _BIGNUMERIC,
                1.0 as _FLOAT64,
                "str" as _STRING,
                TIME '12:30:00.45' as _TIME,
                TIMESTAMP '2014-09-27 12:30:00.45-08' as _TIMESTAMP
            \""",
            bucket="bq-sql-export",
            filename="example/manual.csv",
            export_format="csv",
        )

    """

    def __init__(
        self,
        *,
        use_legacy_sql=False,
        sql_gcp_conn_id="bigquery_default",
        override_schema_from_cursor=True,
        custom_value_transform_delegate=None,
        custom_field_to_bigquer_delegate=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_gcp_conn_id = sql_gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.override_schema_from_cursor = override_schema_from_cursor
        self.custom_value_transform_delegate = custom_value_transform_delegate
        self.custom_field_to_bigquery_delegate = custom_field_to_bigquer_delegate

    def query(self):
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=self.use_legacy_sql)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(self.sql)
        if self.override_schema_from_cursor:
            self.schema = [self.field_to_bigquery(element) for element in cursor.description]
        return _BigQueryCursorWrapper(cursor)
    
    def field_to_bigquery(self, field) -> dict[str, str]:
        if self.custom_field_to_bigquery_delegate is not None:
            return self.custom_field_to_bigquery_delegate(self, field)
        field_type = field[1] if field[1] is not None else "STRING"
        return {
            "name": field[0],
            "type": field_type,
            # Arrays, aka REPEATED, are not supported
            "mode": "NULLABLE" if field[6] else "REQUIRED",
        }

    def convert_type(self, value, schema_type: str, **kwargs):
        if self.custom_value_transform_delegate is not None:
            return self.custom_value_transform_delegate(value, schema_type, **kwargs)
        if value is None or schema_type is None:
            return value
        if schema_type == "BOOLEAN" and isinstance(value, bool):
            return str(value).lower()
        if schema_type == "TIMESTAMP" and isinstance(value, float):
            dt = datetime.fromtimestamp(value, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f %Z")
        return value
