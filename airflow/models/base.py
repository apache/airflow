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

from typing import TYPE_CHECKING, Any

from sqlalchemy import Column, Integer, MetaData, String, text
from sqlalchemy.orm import registry

from airflow import settings
from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin

SQL_ALCHEMY_SCHEMA = conf.get("database", "SQL_ALCHEMY_SCHEMA")

# For more information about what the tokens in the naming convention
# below mean, see:
# https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData.params.naming_convention
naming_convention = {
    "ix": "idx_%(column_0_N_label)s",
    "uq": "%(table_name)s_%(column_0_N_name)s_uq",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}


def get_schema():
    if not SQL_ALCHEMY_SCHEMA or SQL_ALCHEMY_SCHEMA.isspace():
        return None
    return SQL_ALCHEMY_SCHEMA


def _get_schema():
    # Backcompat
    return get_schema()


metadata = MetaData(schema=get_schema(), naming_convention=naming_convention)
mapper_registry = registry(metadata=metadata)
_sentinel = object()

if TYPE_CHECKING:
    Base = Any
else:
    Base = mapper_registry.generate_base()

ID_LEN = 250


def get_id_collation_args():
    """Get SQLAlchemy args to use for COLLATION."""
    collation = conf.get("database", "sql_engine_collation_for_ids", fallback=None)
    if collation:
        return {"collation": collation}
    else:
        # Automatically use utf8mb3_bin collation for mysql
        # This is backwards-compatible. All our IDS are ASCII anyway so even if
        # we migrate from previously installed database with different collation and we end up mixture of
        # COLLATIONS, it's not a problem whatsoever (and we keep it small enough so that our indexes
        # for MYSQL will not exceed the maximum index size.
        #
        # See https://github.com/apache/airflow/pull/17603#issuecomment-901121618.
        #
        # We cannot use session/dialect as at this point we are trying to determine the right connection
        # parameters, so we use the connection
        conn = conf.get("database", "sql_alchemy_conn", fallback="")
        if conn.startswith(("mysql", "mariadb")):
            return {"collation": "utf8mb3_bin"}
        return {}


COLLATION_ARGS: dict[str, Any] = get_id_collation_args()


def StringID(*, length=ID_LEN, **kwargs) -> String:
    return String(length=length, **kwargs, **COLLATION_ARGS)


class TaskInstanceDependencies(Base):
    """Base class for depending models linked to TaskInstance."""

    __abstract__ = True

    task_id = Column(StringID(), nullable=False)
    dag_id = Column(StringID(), nullable=False)
    run_id = Column(StringID(), nullable=False)
    map_index = Column(Integer, nullable=False, server_default=text("-1"))


class AttributeCheckerMeta(type):
    """Metaclass to check attributes of subclasses."""

    def __new__(cls, name, bases, dct):
        """Check that subclasses are setting the required attributes."""
        required_attrs = ["metadata", "migration_dir", "alembic_file", "version_table_name"]
        for attr in required_attrs:
            if attr not in dct:
                raise AttributeError(f"{name} is missing required attribute: {attr}")
        return super().__new__(cls, name, bases, dct)


class BaseDBManager(LoggingMixin, metaclass=AttributeCheckerMeta):
    """Base DB manager for external DBs."""

    metadata: MetaData = None
    migration_dir: str = ""
    alembic_file: str = ""
    version_table_name: str = ""
    # Whether the database supports dropping tables when airflow tables are dropped
    supports_table_dropping: bool = False

    def __init__(self, session):
        super().__init__()
        self.session = session

    def get_alembic_config(self):
        from alembic.config import Config

        config = Config(self.alembic_file)
        config.set_main_option("script_location", self.migration_dir.replace("%", "%%"))
        config.set_main_option("sqlalchemy.url", settings.SQL_ALCHEMY_CONN.replace("%", "%%"))
        return config

    def get_current_revision(self):
        from alembic.migration import MigrationContext

        conn = self.session.connection()

        migration_ctx = MigrationContext.configure(conn, opts={"version_table": self.version_table_name})

        return migration_ctx.get_current_revision()

    def _create_db_from_orm(self):
        """Create database from ORM."""
        from alembic import command

        engine = self.session.get_bind().engine
        self.metadata.create_all(engine)
        config = self.get_alembic_config()
        command.stamp(config, "head")

    def initdb(self):
        """Initialize the database."""
        db_exists = self.get_current_revision()
        if db_exists:
            self.upgradedb()
        else:
            self._create_db_from_orm()

    def upgradedb(self, to_version=None, from_version=None, show_sql_only=False):
        """Upgrade the database."""
        from alembic import command

        config = self.get_alembic_config()
        command.upgrade(config, revision=to_version or "heads", sql=show_sql_only)

    def downgradedb(self, to_version, from_version=None, show_sql_only=False):
        """Downgrade the database."""
