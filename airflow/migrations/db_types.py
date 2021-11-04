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
#
import sys

import sqlalchemy as sa
from alembic import context
from lazy_object_proxy import Proxy

######################################
# Note about this module:
#
# It loads the specific type dynamically at runtime. For IDE/typing support
# there is an associated db_types.pyi. If you add a new type in here, add a
# simple version in there too.
######################################


def _mssql_use_date_time2():
    conn = context.get_bind()
    result = conn.execute(
        """SELECT CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '8%' THEN '2000' WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
        like '9%' THEN '2005' ELSE '2005Plus' END AS MajorVersion"""
    ).fetchone()
    mssql_version = result[0]
    return mssql_version not in ("2000", "2005")


MSSQL_USE_DATE_TIME2 = Proxy(_mssql_use_date_time2)


def _mssql_TIMESTAMP():
    from sqlalchemy.dialects import mssql

    return mssql.DATETIME2(precision=6) if MSSQL_USE_DATE_TIME2 else mssql.DATETIME


def _mysql_TIMESTAMP():
    from sqlalchemy.dialects import mysql

    return mysql.TIMESTAMP(fsp=6, timezone=True)


def _sa_TIMESTAMP():
    return sa.TIMESTAMP(timezone=True)


def _sa_StringID():
    from airflow.models.base import StringID

    return StringID


def __getattr__(name):
    if name in ["TIMESTAMP", "StringID"]:
        dialect = context.get_bind().dialect.name
        module = globals()

        # Lookup the type based on the dialect specific type, or fallback to the generic type
        type_ = module.get(f'_{dialect}_{name}', None) or module.get(f'_sa_{name}')
        val = module[name] = type_()
        return val

    raise AttributeError(f"module {__name__} has no attribute {name}")


if sys.version_info < (3, 7):
    from pep562 import Pep562

    Pep562(__name__)
