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

import os
from typing import TYPE_CHECKING

from alembic import command
from sqlalchemy import inspect

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from alembic.script import ScriptDirectory
    from sqlalchemy import MetaData


class BaseDBManager(LoggingMixin):
    """Abstract Base DB manager for external DBs."""

    metadata: MetaData
    migration_dir: str
    alembic_file: str
    version_table_name: str
    # Whether the database supports dropping tables when airflow tables are dropped
    supports_table_dropping: bool = False

    def __init__(self, session):
        super().__init__()
        self.session = session

    def get_alembic_config(self):
        from alembic.config import Config

        from airflow import settings

        config = Config(self.alembic_file)
        config.set_main_option("script_location", self.migration_dir.replace("%", "%%"))
        config.set_main_option("sqlalchemy.url", settings.SQL_ALCHEMY_CONN.replace("%", "%%"))
        return config

    def get_script_object(self, config=None) -> ScriptDirectory:
        from alembic.script import ScriptDirectory

        if not config:
            config = self.get_alembic_config()
        return ScriptDirectory.from_config(config)

    def _get_migration_ctx(self):
        from alembic.migration import MigrationContext

        conn = self.session.connection()

        return MigrationContext.configure(conn, opts={"version_table": self.version_table_name})

    def get_current_revision(self):
        return self._get_migration_ctx().get_current_revision()

    def check_migration(self):
        """Check migration done."""
        script_heads = self.get_script_object().get_heads()
        db_heads = self.get_current_revision()
        if db_heads:
            db_heads = {db_heads}
        if not db_heads and not script_heads:
            return True
        if set(script_heads) == db_heads:
            return True
        return False

    def _create_db_from_orm(self):
        """Create database from ORM."""
        self.log.info("Creating %s tables from the ORM", self.__class__.__name__)
        engine = self.session.get_bind().engine
        self.metadata.create_all(engine)
        config = self.get_alembic_config()
        command.stamp(config, "head")
        self.log.info("%s tables have been created from the ORM", self.__class__.__name__)

    def drop_tables(self, connection):
        self.metadata.drop_all(connection)
        version = self._get_migration_ctx()._version
        if inspect(connection).has_table(version.name):
            version.drop(connection)

    def resetdb(self, skip_init=False):
        from airflow.utils.db import DBLocks, create_global_lock

        connection = settings.engine.connect()

        with create_global_lock(self.session, lock=DBLocks.MIGRATIONS), connection.begin():
            self.log.info("Dropping %s tables", self.__class__.__name__)
            self.drop_tables(connection)
        if not skip_init:
            self.initdb()

    def initdb(self):
        """Initialize the database."""
        db_exists = self.get_current_revision()
        if db_exists:
            self.upgradedb()
        else:
            self._create_db_from_orm()

    def upgradedb(self, to_revision=None, from_revision=None, show_sql_only=False):
        """Upgrade the database."""
        self.log.info("Upgrading the %s database", self.__class__.__name__)

        config = self.get_alembic_config()
        command.upgrade(config, revision=to_revision or "heads", sql=show_sql_only)
        self.log.info("Migrated the %s database", self.__class__.__name__)

    def downgrade(self, to_version, from_version=None, show_sql_only=False):
        """Downgrade the database."""
        raise NotImplementedError


class RunDBManager(LoggingMixin):
    """
    Run External DB Managers.

    Validates and runs the external database managers.
    """

    def __init__(self):
        super().__init__()
        self._managers: list[BaseDBManager] = []
        managers = conf.get("database", "external_db_managers").split(",")
        for module in managers:
            manager = import_string(module)
            self._managers.append(manager)

    def validate(self):
        """Validate the external database managers."""
        for manager in self._managers:
            RunDBManager._validate(manager)

    @staticmethod
    def _validate(manager: BaseDBManager):
        """Validate the external database migration."""
        import ast

        from airflow.models.base import metadata as airflow_metadata

        external_metadata = manager.metadata
        airflow_m = airflow_metadata
        # validate tables are not airflow tables in metadata
        for table_ in external_metadata.tables:
            if table_ in airflow_m.tables:
                raise AirflowException(f"Table '{table_}' already exists in the Airflow metadata")
        # validate the version table schema is set appropriately in env.py
        migration_dir = manager.migration_dir
        env_file = os.path.join(migration_dir, "env.py")
        with open(env_file) as f:
            tree = ast.parse(f.read(), filename=env_file)

        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "context.configure"
            ):
                if "version_table" not in node.keywords:
                    raise AirflowException(f"version_table not set in {env_file}")
        # validate the version table is not airflow version table
        if manager.version_table_name == "alembic_version":
            raise AirflowException(f"{manager}.version_table_name cannot be 'alembic_version'")

    def check_migration(self, session):
        """Check the external database migration."""
        return_value = []
        for manager in self._managers:
            m = manager(session)
            return_value.append(m.check_migration)
        return all([x() for x in return_value])

    def initdb(self, session):
        """Initialize the external database managers."""
        for manager in self._managers:
            m = manager(session)
            m.initdb()

    def upgradedb(self, session):
        """Upgrade the external database managers."""
        for manager in self._managers:
            m = manager(session)
            m.upgradedb()

    def downgrade(self, session):
        """Downgrade the external database managers."""
        for manager in self._managers:
            m = manager(session)
            m.downgrade()

    def drop_tables(self, session, connection):
        """Drop the external database managers."""
        for manager in self._managers:
            if manager.supports_table_dropping:
                m = manager(session)
                m.drop_tables(connection)
