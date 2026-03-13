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

import base64
import calendar
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import cached_property
from typing import TYPE_CHECKING

import oracledb

from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage


class OracleToGCSOperator(BaseSQLToGCSOperator):
    """
    Copy data from Oracle to Google Cloud Storage in JSON, CSV or Parquet format.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OracleToGCSOperator`

    :param oracle_conn_id: Reference to a specific
        :ref:`Oracle hook <howto/connection:oracle>`.
    :param ensure_utc: Ensure TIMESTAMP columns exported as UTC. If set to
        `False`, TIMESTAMP columns will be exported using the Oracle server's
        default timezone.
    """

    ui_color = "#a0e08c"

    type_map = {
        oracledb.DB_TYPE_BINARY_DOUBLE: "DECIMAL",
        oracledb.DB_TYPE_BINARY_FLOAT: "DECIMAL",
        oracledb.DB_TYPE_BINARY_INTEGER: "INTEGER",
        oracledb.DB_TYPE_BOOLEAN: "BOOLEAN",
        oracledb.DB_TYPE_DATE: "TIMESTAMP",
        oracledb.DB_TYPE_NUMBER: "NUMERIC",
        oracledb.DB_TYPE_TIMESTAMP: "TIMESTAMP",
        oracledb.DB_TYPE_TIMESTAMP_LTZ: "TIMESTAMP",
        oracledb.DB_TYPE_TIMESTAMP_TZ: "TIMESTAMP",
    }

    def __init__(self, *, oracle_conn_id="oracle_default", ensure_utc=False, **kwargs):
        super().__init__(**kwargs)
        self.ensure_utc = ensure_utc
        self.oracle_conn_id = oracle_conn_id

    @cached_property
    def db_hook(self) -> OracleHook:
        return OracleHook(oracle_conn_id=self.oracle_conn_id)

    def query(self):
        """Query Oracle and returns a cursor to the results."""
        conn = self.db_hook.get_conn()
        cursor = conn.cursor()
        if self.ensure_utc:
            # Ensure TIMESTAMP results are in UTC
            tz_query = "SET time_zone = '+00:00'"
            self.log.info("Executing: %s", tz_query)
            cursor.execute(tz_query)
        self.log.info("Executing: %s", self.sql)
        cursor.execute(self.sql)
        return cursor

    def field_to_bigquery(self, field) -> dict[str, str]:
        field_type = self.type_map.get(field[1], "STRING")

        field_mode = "NULLABLE" if not field[6] or field_type == "TIMESTAMP" else "REQUIRED"
        return {
            "name": field[0],
            "type": field_type,
            "mode": field_mode,
        }

    def convert_type(self, value, schema_type, **kwargs):
        """
        Take a value from Oracle db and convert it to a value safe for JSON/Google Cloud Storage/BigQuery.

        * Datetimes are converted to UTC seconds.
        * Decimals are converted to floats.
        * Dates are converted to ISO formatted string if given schema_type is
          DATE, or UTC seconds otherwise.
        * Binary type fields are converted to integer if given schema_type is
          INTEGER, or encoded with base64 otherwise. Imported BYTES data must
          be base64-encoded according to BigQuery documentation:
          https://cloud.google.com/bigquery/data-types

        :param value: Oracle db column value
        :param schema_type: BigQuery data type
        """
        if value is None:
            return value
        if isinstance(value, datetime):
            value = calendar.timegm(value.timetuple())
        elif isinstance(value, timedelta):
            value = value.total_seconds()
        elif isinstance(value, Decimal):
            value = float(value)
        elif isinstance(value, date):
            if schema_type == "DATE":
                value = value.isoformat()
            else:
                value = calendar.timegm(value.timetuple())
        elif isinstance(value, bytes):
            if schema_type == "INTEGER":
                value = int.from_bytes(value, "big")
            else:
                value = base64.standard_b64encode(value).decode("ascii")
        return value

    def get_openlineage_facets_on_start(self) -> OperatorLineage | None:
        from airflow.providers.common.compat.openlineage.facet import SQLJobFacet
        from airflow.providers.common.compat.openlineage.utils.sql import get_openlineage_facets_with_sql
        from airflow.providers.openlineage.extractors import OperatorLineage

        sql_parsing_result = get_openlineage_facets_with_sql(
            hook=self.db_hook,
            sql=self.sql,
            conn_id=self.oracle_conn_id,
            database=self.db_hook.service_name or self.db_hook.sid,
        )
        gcs_output_datasets = self._get_openlineage_output_datasets()
        if sql_parsing_result:
            sql_parsing_result.outputs = gcs_output_datasets
            return sql_parsing_result
        return OperatorLineage(outputs=gcs_output_datasets, job_facets={"sql": SQLJobFacet(self.sql)})
