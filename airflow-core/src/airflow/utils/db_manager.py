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

import inspect as _inspect
import os
from typing import TYPE_CHECKING

from alembic import command
from sqlalchemy import inspect

from airflow import settings
from airflow._shared.module_loading import import_string
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import get_dialect_name

if TYPE_CHECKING:
    from alembic.script import ScriptDirectory
    from sqlalchemy import MetaData


def _callable_accepts_use_migration_files(callable_) -> bool:
    """Return True if *callable_* accepts a ``use_migration_files`` keyword argument."""
    try:
        signature = _inspect.signature(callable_)
    except (TypeError, ValueError):
        return False

    if "use_migration_files" in signature.parameters:
        return True

    return any(
        parameter.kind == _inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
    )


class BaseDBManager(LoggingMixin):
    """Abstract Base DB manager for external DBs."""

    metadata: MetaData
    migration_dir: str
    alembic_file: str
    version_table_name: str
    # Whether the database supports dropping tables when airflow tables are dropped
    supports_table_dropping: bool = False
    revision_heads_map: dict[str, str] = {}

    def __init__(self, session):
        super().__init__()
        self.session = session

    def _release_metadata_locks_if_needed(self) -> None:
        """
        Release MySQL metadata locks by committing the session.

        MySQL requires metadata locks to be released before DDL operations.
        This is done by committing the current transaction.
        This method is a no-op for non-MySQL databases.
        """
        if get_dialect_name(self.session) != "mysql":
            return

        self.log.debug("MySQL: Releasing metadata locks for DDL operations")
        self.session.commit()
        self.log.debug("MySQL: Session committed, metadata locks released")

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

    def create_db_from_orm(self):
        """Create database from ORM."""
        self.log.info("Creating %s tables from the ORM", self.__class__.__name__)
        self._release_metadata_locks_if_needed()
        engine = self.session.get_bind().engine
        self.metadata.create_all(engine)
        config = self.get_alembic_config()
        command.stamp(config, "head")
        self.log.info("%s tables have been created from the ORM", self.__class__.__name__)

    def drop_tables(self, connection):
        if not self.supports_table_dropping:
            return
        self.metadata.drop_all(connection)
        version = self._get_migration_ctx()._version
        if inspect(connection).has_table(version.name):
            version.drop(connection)

    def resetdb(self, skip_init=False, use_migration_files=False):
        from airflow.utils.db import DBLocks, create_global_lock

        self._release_metadata_locks_if_needed()

        connection = settings.engine.connect()

        with create_global_lock(self.session, lock=DBLocks.MIGRATIONS), connection.begin():
            self.log.info("Dropping %s tables", self.__class__.__name__)
            self.drop_tables(connection)
        if not skip_init:
            if _callable_accepts_use_migration_files(self.initdb):
                self.initdb(use_migration_files=use_migration_files)
            else:
                self.initdb()

    def initdb(self, use_migration_files=False):
        """Initialize the database."""
        self._release_metadata_locks_if_needed()
        db_exists = self.get_current_revision()
        if db_exists or use_migration_files:
            if _callable_accepts_use_migration_files(self.upgradedb):
                self.upgradedb(use_migration_files=use_migration_files)
            else:
                self.upgradedb()
        else:
            self.create_db_from_orm()

    def upgradedb(self, to_revision=None, from_revision=None, show_sql_only=False, use_migration_files=False):
        """Upgrade the database."""
        self.log.info("Upgrading the %s database", self.__class__.__name__)

        self._release_metadata_locks_if_needed()
        current_revision = self.get_current_revision()
        # MySQL can reacquire metadata locks during the revision lookup above.
        # Release them again before Alembic opens its migration connection.
        self._release_metadata_locks_if_needed()

        if not current_revision and not to_revision and not use_migration_files and not show_sql_only:
            self.create_db_from_orm()
            return

        config = self.get_alembic_config()
        command.upgrade(config, revision=to_revision or "heads", sql=show_sql_only)
        self.log.info("Migrated the %s database", self.__class__.__name__)

    def downgrade(self, to_revision, from_revision=None, show_sql_only=False):
        """Downgrade the database."""
        raise NotImplementedError


class RunDBManager(LoggingMixin):
    """
    Run External DB Managers.

    Validates and runs the external database managers.
    """

    def __init__(self):
        from airflow.providers_manager import ProvidersManager

        super().__init__()
        self._managers: list[BaseDBManager] = []

        # Start with auto-discovered DB managers from installed providers.
        # ProvidersManager reads the ``db-managers`` key from each provider's
        # get_provider_info() and is the primary source of truth.
        managers: list[str] = list(ProvidersManager().db_managers)

        # Add any explicitly configured managers not already discovered.
        managers_config = conf.get("database", "external_db_managers", fallback=None)
        if managers_config:
            for m in managers_config.split(","):
                if stripped := m.strip():
                    if stripped not in managers:
                        managers.append(stripped)

        # Add the DB manager declared by the configured auth manager as a
        # final fallback for backward compatibility.
        # This is wrapped in a try/except because in migration-only contexts
        # (e.g. the Helm migrateDatabaseJob) the auth manager may not be fully
        # initializable — a Flask app context or other runtime state may be
        # absent.  A failure here must not silently drop the auth manager's DB
        # manager from the migration list; ProvidersManager discovery above is
        # the reliable path in those contexts.
        try:
            from airflow.api_fastapi.app import create_auth_manager

            auth_manager_db_manager = create_auth_manager().get_db_manager()
            if auth_manager_db_manager and auth_manager_db_manager not in managers:
                managers.append(auth_manager_db_manager)
        except Exception:
            self.log.debug(
                "Could not retrieve DB manager from auth manager during RunDBManager "
                "initialisation. This is expected in migration-only contexts where the "
                "auth manager cannot be fully initialised. DB managers discovered via "
                "ProvidersManager will still be used.",
                exc_info=True,
            )

        for module in managers:
            manager = import_string(module.strip())
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

    def _call_with_optional_use_migration_files(
        self, manager_instance: BaseDBManager, method_name: str, use_migration_files: bool
    ) -> None:
        method = getattr(manager_instance, method_name)
        if _callable_accepts_use_migration_files(method):
            method(use_migration_files=use_migration_files)
            return

        if use_migration_files:
            self.log.warning(
                "External DB manager %s.%s does not support 'use_migration_files'; proceeding without it.",
                type(manager_instance).__name__,
                method_name,
            )
        method()

    def initdb(self, session, use_migration_files=False):
        """Initialize the external database managers."""
        for manager in self._managers:
            m = manager(session)
            self._call_with_optional_use_migration_files(m, "initdb", use_migration_files)

    def upgradedb(self, session, use_migration_files=False):
        """Upgrade the external database managers."""
        for manager in self._managers:
            m = manager(session)
            self._call_with_optional_use_migration_files(m, "upgradedb", use_migration_files)

    def drop_tables(self, session, connection):
        """Drop the external database managers."""
        for manager in self._managers:
            m = manager(session)
            m.drop_tables(connection)
