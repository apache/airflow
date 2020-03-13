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
import base64
import calendar
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from airflow.providers.google.cloud.operators.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.presto.hooks.presto import PrestoHook
from airflow.utils.decorators import apply_defaults


class _AirflowPrestoCursorAdapter:
    """
    An adapter that adds additional feature to the Presto cursor.

    The implementation of cursor in the prestodb library is not sufficient.
    The following changes have been made:

    * The poke mechanism for row. You can look at the next row without consuming it.
    * The description attribute is available before reading the first row. Thanks to the poke mechanism.
    * the iterator interface has been implemented.

    A detailed description of the class methods is available in
    `PEP-249 <https://www.python.org/dev/peps/pep-0249/>`__.
    """

    def __init__(self, cursor):
        self.cursor = cursor
        self.rows = []
        self.initialized = False

    @property
    def description(self):
        if not self.initialized:
            # Peek for first row to load description.
            self.peekone()
        return self.cursor.description

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def close(self):
        return self.cursor.close()

    def execute(self, *args, **kwwargs):
        self.initialized = False
        self.rows = []
        return self.cursor.execute(*args, **kwwargs)

    def executemany(self, *args, **kwargs):
        self.initialized = False
        self.rows = []
        return self.cursor.executemany(*args, **kwargs)

    def peekone(self):
        """
        Return the next row without consuming it.
        """
        self.initialized = True
        element = self.cursor.fetchone()
        self.rows.insert(0, element)
        return element

    def fetchone(self):
        if self.rows:
            return self.rows.pop(0)
        return self.cursor.fetchone()

    def fetchmany(self, size=None):
        if size is None:
            size = self.cursor.arraysize

        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def __next__(self):
        result = self.fetchone()
        if result is None:
            raise StopIteration()
        return result

    def __iter__(self):
        return self


class PrestoToGCSOperator(BaseSQLToGCSOperator):
    """Copy data from PrestoDB to Google Cloud Storage in JSON or CSV format.

    :param presto_conn_id: Reference to a specific MySQL hook.
    :type presto_conn_id: str
    """

    ui_color = "#a0e08c"

    type_map = {
        "BOOLEAN": "BOOL",
        "TINYINT": "INT64",
        "SMALLINT": "INT64",
        "INTEGER": "INT64",
        "BIGINT": "INT64",
        "REAL": "FLOAT64",
        "DOUBLE": "FLOAT64",
        "DECIMAL": "NUMERIC",
        "VARCHAR": "STRING",
        "CHAR": "STRING",
        "VARBINARY": "BYTES",
        "JSON": "STRING",
        "DATE": "DATE",
        "TIME": "TIME",
        # BigQuery don't time with timezone native.
        "TIME WITH TIME ZONE": "STRING",
        "TIMESTAMP": "TIMESTAMP",
        # BigQuery supports a narrow range of time zones during import.
        # You should use TIMESTAMP function, if you wanna have TIMESTAMP type
        "TIMESTAMP WITH TIME ZONE": "STRING",
        "IPADDRESS": "STRING",
        "UUID": "STRING",
    }

    @apply_defaults
    def __init__(self, presto_conn_id="presto_default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.presto_conn_id = presto_conn_id

    def query(self):
        """
        Queries mysql and returns a cursor to the results.
        """
        presto = PrestoHook(presto_conn_id=self.presto_conn_id)
        conn = presto.get_conn()
        cursor = conn.cursor()
        self.log.info("Executing: %s", self.sql)
        cursor.execute(self.sql)
        return _AirflowPrestoCursorAdapter(cursor)

    def field_to_bigquery(self, field):
        """Convert presto field type to BigQuery field type."""
        clear_field_type = field[1].upper()
        # remove type argument e.g. DECIMAL(2, 10) => DECIMAL
        clear_field_type, _, _ = clear_field_type.partition("(")
        new_field_type = self.type_map.get(clear_field_type, "STRING")

        return {"name": field[0], "type": new_field_type}

    def convert_type(self, value, schema_type):
        """
        Do nothing. Presto uses JSON on the transport layer, so types are simple.

        :param value: Presto column value
        :type value: Any
        :param schema_type: BigQuery data type
        :type schema_type: str
        """
        return value
