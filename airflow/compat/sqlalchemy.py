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

from sqlalchemy import Table
from sqlalchemy.engine import Connection

try:
    from sqlalchemy import inspect
except AttributeError:
    from sqlalchemy.engine.reflection import Inspector

    inspect = Inspector.from_engine

__all__ = ["has_table", "inspect"]


def has_table(conn: Connection, table: Table):
    try:
        return inspect(conn).has_table(table)
    except AttributeError:
        return table.exists(conn)
