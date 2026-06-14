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
"""MongoDB to GCS operator."""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from functools import cached_property
from typing import Any

from bson import ObjectId
from bson.decimal128 import Decimal128

from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


class _MongoCursorAdapter:
    """
    Wrap a pymongo cursor as a DB-API 2.0 style cursor.

    ``BaseSQLToGCSOperator`` consumes ``cursor.description`` to derive the
    BigQuery schema and iterates the cursor expecting tuples. MongoDB documents
    are dict-shaped and have no fixed schema; this adapter:

    * Peeks the first document to derive ``description`` (column names and
      Python types).
    * Re-yields the first document, then iterates the rest.
    * Converts each document to a tuple in the derived column order, filling
      missing fields with ``None``.
    """

    def __init__(self, cursor: Any) -> None:
        self._cursor = iter(cursor)
        self._first: dict | None = None
        self._description: list[tuple] = []
        try:
            self._first = next(self._cursor)
        except StopIteration:
            return
        self._description = [
            (name, type(value), None, None, None, None, True) for name, value in self._first.items()
        ]

    @property
    def description(self) -> list[tuple]:
        return self._description

    def __iter__(self) -> Iterator[tuple]:
        if self._first is None:
            return
        names = [d[0] for d in self._description]
        yield tuple(self._first.get(n) for n in names)
        for doc in self._cursor:
            yield tuple(doc.get(n) for n in names)


class MongoToGCSOperator(BaseSQLToGCSOperator):
    """
    Copy data from MongoDB to Google Cloud Storage in JSON, CSV or Parquet format.

    .. note::
        MongoDB is a NoSQL store, so subclassing ``BaseSQLToGCSOperator`` is a
        deliberate reuse choice rather than a natural fit. The base class already
        implements the chunking, schema inference and GCS upload flow we want; this
        operator reuses it by adapting the pymongo cursor to a DB-API style cursor
        (see :class:`_MongoCursorAdapter`) and overriding ``query`` /
        ``field_to_bigquery`` / ``convert_type``. A dedicated
        ``BaseNoSQLToGCSOperator`` could be a cleaner home for this in the future.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MongoToGCSOperator`

    :param mongo_conn_id: Reference to a specific
        :ref:`Mongo connection <howto/connection:mongo>`.
    :param mongo_db: The MongoDB database name.
    :param mongo_collection: The MongoDB collection name.
    :param mongo_query: A MongoDB find filter (``dict``) or aggregation pipeline
        (``list``). Defaults to ``{}`` (match all).
    :param mongo_projection: Optional projection passed to ``find()``. Ignored
        when ``mongo_query`` is an aggregation pipeline. Accepts a dict
        (``{"field": 1}``) or list of field names.
    :param allow_disk_use: Whether to pass ``allowDiskUse=True`` to
        ``aggregate()``. Defaults to True.
    """

    ui_color = "#a0e08c"

    # ``sql``, ``template_ext`` (``.sql``) and the ``sql`` renderer are inherited
    # from ``BaseSQLToGCSOperator`` but are meaningless here — this operator is
    # driven by ``mongo_query``, not SQL. Override them explicitly so the unused
    # ``sql`` field is not exposed in rendered templates.
    template_fields: Sequence[str] = (
        "bucket",
        "filename",
        "schema_filename",
        "schema",
        "parameters",
        "impersonation_chain",
        "partition_columns",
        "mongo_collection",
        "mongo_db",
        "mongo_query",
    )
    template_ext: Sequence[str] = ()
    template_fields_renderers = {}

    type_map: dict[type, str] = {
        bool: "BOOL",
        int: "INTEGER",
        float: "FLOAT",
        Decimal: "FLOAT",
        Decimal128: "FLOAT",
        str: "STRING",
        bytes: "BYTES",
        ObjectId: "STRING",
        datetime: "TIMESTAMP",
        date: "DATE",
        time: "TIME",
        list: "STRING",
        dict: "STRING",
    }

    def __init__(
        self,
        *,
        mongo_conn_id: str = "mongo_default",
        mongo_db: str,
        mongo_collection: str,
        mongo_query: dict | list | None = None,
        mongo_projection: dict | list | None = None,
        allow_disk_use: bool = True,
        **kwargs: Any,
    ) -> None:
        # `sql` is required by BaseSQLToGCSOperator but is unused here.
        kwargs.setdefault("sql", "")
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_query = mongo_query if mongo_query is not None else {}
        self.mongo_projection = mongo_projection
        self.allow_disk_use = allow_disk_use

    @cached_property
    def db_hook(self) -> MongoHook:
        return MongoHook(mongo_conn_id=self.mongo_conn_id)

    def query(self) -> _MongoCursorAdapter:
        """Execute the configured find/aggregate and return a DB-API style cursor."""
        coll = self.db_hook.get_conn()[self.mongo_db][self.mongo_collection]
        cursor: Any
        if isinstance(self.mongo_query, list):
            self.log.info("Executing aggregate: %s", self.mongo_query)
            cursor = coll.aggregate(self.mongo_query, allowDiskUse=self.allow_disk_use)
        else:
            self.log.info(
                "Executing find: filter=%s projection=%s",
                self.mongo_query,
                self.mongo_projection,
            )
            cursor = coll.find(self.mongo_query, projection=self.mongo_projection)
        return _MongoCursorAdapter(cursor)

    def field_to_bigquery(self, field) -> dict[str, str]:
        return {
            "name": field[0],
            "type": self.type_map.get(field[1], "STRING"),
            "mode": "NULLABLE",
        }

    def convert_type(self, value: Any, schema_type: str | None, **kwargs: Any) -> Any:
        """
        Convert pymongo values to BigQuery-friendly types.

        * ``ObjectId`` -> ``str``.
        * ``Decimal128`` / ``Decimal`` -> ``float``.
        * ``bytes`` -> base64-encoded ``str`` (or ``int`` when
          ``schema_type == 'INTEGER'``).
        * ``datetime`` -> ``str(value)``.
        * ``date`` -> ISO date string when ``schema_type == 'DATE'``, otherwise
          combined ``datetime`` string.
        * ``list`` / ``dict`` / ``tuple`` -> JSON string.
        """
        if value is None:
            return value
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, Decimal128):
            return float(value.to_decimal())
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, bytes):
            if schema_type == "INTEGER":
                return int.from_bytes(value, "big")
            return base64.standard_b64encode(value).decode("ascii")  # type:ignore
        if isinstance(value, datetime):
            return str(value)
        if isinstance(value, date):
            if schema_type == "DATE":
                return value.isoformat()
            return str(datetime.combine(value, time.min))
        if isinstance(value, (list, dict, tuple)):
            return json.dumps(value, default=str)
        return value
