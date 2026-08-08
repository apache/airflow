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

import oracledb


def _read_lob(val):
    if isinstance(val, oracledb.LOB):
        return val.read()
    return val


def _read_lobs(row):
    if row is not None:
        return tuple([_read_lob(value) for value in row])
    return row


def fetch_all_handler(cursor) -> list[tuple] | None:
    """Return results for DbApiHook.run(). If oracledb.LOB objects are present, then those will be read."""
    if not hasattr(cursor, "description"):
        raise RuntimeError(
            "The database we interact with does not support DBAPI 2.0. Use operator and "
            "handlers that are specifically designed for your database."
        )
    if cursor.description is not None:
        results = [_read_lobs(row) for row in cursor.fetchall()]
        return results
    return None


def fetch_one_handler(cursor) -> tuple | None:
    """Return first result for DbApiHook.run(). If oracledb.LOB objects are present, then those will be read."""
    if not hasattr(cursor, "description"):
        raise RuntimeError(
            "The database we interact with does not support DBAPI 2.0. Use operator and "
            "handlers that are specifically designed for your database."
        )
    if cursor.description is not None:
        return _read_lobs(cursor.fetchone())
    return None
