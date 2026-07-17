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


def _mssql_TIMESTAMP():
    from sqlalchemy.dialects import mssql

    class DATETIME2(mssql.DATETIME2):
        def __init__(self, *args, precision=6, **kwargs):
            super().__init__(*args, precision=precision, **kwargs)

    return DATETIME2


def _mysql_TIMESTAMP():
    from sqlalchemy.dialects import mysql

    class TIMESTAMP(mysql.TIMESTAMP):
        def __init__(self, *args, fsp=6, timezone=True, **kwargs):
            super().__init__(*args, fsp=fsp, timezone=timezone, **kwargs)

    return TIMESTAMP


def _sa_TIMESTAMP():
    class TIMESTAMP(sa.TIMESTAMP):
        def __init__(self, *args, timezone=True, **kwargs):
            super().__init__(*args, timezone=timezone, **kwargs)

    return TIMESTAMP


def _sa_StringID():
    from airflow.models.base import StringID

    return StringID


def __getattr__(name):
    if name in ["TIMESTAMP", "StringID"]:

        def lazy_load():
            dialect = context.get_bind().dialect.name
            module = globals()

            # Lookup the type based on the dialect specific type, or fallback to the generic type
            type_ = module.get(f"_{dialect}_{name}", None) or module.get(f"_sa_{name}")
            val = module[name] = type_()
            return val

        # Prior to v1.4 of our Helm chart we didn't correctly initialize the Migration environment, so
        # `context.get_bind()` would fail if called at the top level. To make it easier on migration writers
        # we make the returned objects lazy.
        return Proxy(lazy_load)

    raise AttributeError(f"module {__name__} has no attribute {name}")
