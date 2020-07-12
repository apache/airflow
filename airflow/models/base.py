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

from typing import Any

from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from airflow.configuration import conf

SQL_ALCHEMY_SCHEMA = conf.get("core", "SQL_ALCHEMY_SCHEMA")

# Recommended naming convention used by Alembic, as various different database
# providers will autogenerate vastly different names making migrations more
# difficult. See: https://alembic.sqlalchemy.org/en/latest/naming.html
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

metadata = (
    None
    if not SQL_ALCHEMY_SCHEMA or SQL_ALCHEMY_SCHEMA.isspace()
    else MetaData(schema=SQL_ALCHEMY_SCHEMA, naming_convention=NAMING_CONVENTION)
)
Base = declarative_base(metadata=metadata)  # type: Any

ID_LEN = 250


# used for typing
class Operator:
    """ Class just used for Typing """


def get_id_collation_args():
    """ Get SQLAlchemy args to use for COLLATION """
    collation = conf.get('core', 'sql_engine_collation_for_ids', fallback=None)
    if collation:
        return {'collation': collation}
    else:
        return {}


COLLATION_ARGS = get_id_collation_args()
