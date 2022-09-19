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

from typing import Any

from prestodb.client import PrestoResult
from prestodb.dbapi import Cursor as PrestoCursor

from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.presto.hooks.presto import PrestoHook


class _PrestoToGCSPrestoCursorAdapter:
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

    def __init__(self, cursor: PrestoCursor):
        self.cursor: PrestoCursor = cursor
        self.rows: list[Any] = []
        self.initialized: bool = False

    @property
    def description(self) -> list[tuple]:
        """
        This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        * ``name``
        * ``type_code``
        * ``display_size``
        * ``internal_size``
        * ``precision``
        * ``scale``
        * ``null_ok``

        The first two items (``name`` and ``type_code``) are mandatory, the other
        five are optional and are set to None if no meaningful values can be provided.
        """
        if not self.initialized:
            # Peek for first row to load description.
            self.peekone()
        return self.cursor.description

    @property
    def rowcount(self) -> int:
        """The read-only attribute specifies the number of rows"""
        return self.cursor.rowcount

    def close(self) -> None:
        """Close the cursor now"""
        self.cursor.close()

    def execute(self, *args, **kwargs) -> PrestoResult:
        """Prepare and execute a database operation (query or command)."""
        self.initialized = False
        self.rows = []
        return self.cursor.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        """
        Prepare a database operation (query or command) and then execute it against all parameter
        sequences or mappings found in the sequence seq_of_parameters.
        """
        self.initialized = False
        self.rows = []
        return self.cursor.executemany(*args, **kwargs)

    def peekone(self) -> Any:
        """Return the next row without consuming it."""
        self.initialized = True
        element = self.cursor.fetchone()
        self.rows.insert(0, element)
        return element

    def fetchone(self) -> Any:
        """
        Fetch the next row of a query result set, returning a single sequence, or
        ``None`` when no more data is available.
        """
        if self.rows:
            return self.rows.pop(0)
        return self.cursor.fetchone()

    def fetchmany(self, size=None) -> list:
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences
        (e.g. a list of tuples). An empty sequence is returned when no more rows are available.
        """
        if size is None:
            size = self.cursor.arraysize

        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def __next__(self) -> Any:
        """
        Return the next row from the currently executing SQL statement using the same semantics as
        ``.fetchone()``.  A ``StopIteration`` exception is raised when the result set is exhausted.
        :return:
        """
        result = self.fetchone()
        if result is None:
            raise StopIteration()
        return result

    def __iter__(self) -> _PrestoToGCSPrestoCursorAdapter:
        """Return self to make cursors compatible to the iteration protocol"""
        return self


class PrestoToGCSOperator(BaseSQLToGCSOperator):
    """Copy data from PrestoDB to Google Cloud Storage in JSON, CSV or Parquet format.

    :param presto_conn_id: Reference to a specific Presto hook.
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
        # You should use TIMESTAMP function, if you want have TIMESTAMP type
        "TIMESTAMP WITH TIME ZONE": "STRING",
        "IPADDRESS": "STRING",
        "UUID": "STRING",
    }

    def __init__(self, *, presto_conn_id: str = "presto_default", **kwargs):
        super().__init__(**kwargs)
        self.presto_conn_id = presto_conn_id

    def query(self):
        """Queries presto and returns a cursor to the results."""
        presto = PrestoHook(presto_conn_id=self.presto_conn_id)
        conn = presto.get_conn()
        cursor = conn.cursor()
        self.log.info("Executing: %s", self.sql)
        cursor.execute(self.sql)
        return _PrestoToGCSPrestoCursorAdapter(cursor)

    def field_to_bigquery(self, field) -> dict[str, str]:
        """Convert presto field type to BigQuery field type."""
        clear_field_type = field[1].upper()
        # remove type argument e.g. DECIMAL(2, 10) => DECIMAL
        clear_field_type, _, _ = clear_field_type.partition("(")
        new_field_type = self.type_map.get(clear_field_type, "STRING")

        return {"name": field[0], "type": new_field_type}

    def convert_type(self, value, schema_type, **kwargs):
        """
        Do nothing. Presto uses JSON on the transport layer, so types are simple.

        :param value: Presto column value
        :param schema_type: BigQuery data type
        """
        return value
