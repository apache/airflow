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

import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect

from airflow.providers.edge3.models.edge_base import edge_metadata
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel
from airflow.utils.db_manager import BaseDBManager

if TYPE_CHECKING:
    from sqlalchemy.engine import Inspector

try:
    from airflow.utils.db_manager import _callable_accepts_use_migration_files
except ImportError:
    # Older Airflow versions do not have this helper; those versions also do not
    # accept use_migration_files on BaseDBManager methods, so always return False.
    def _callable_accepts_use_migration_files(callable_: Any) -> bool:
        return False


PACKAGE_DIR = Path(__file__).parents[1]

_REVISION_HEADS_MAP: dict[str, str] = {
    "3.0.0": "9d34dfc2de06",
    "3.2.0": "8c275b6fbaa8",
}


class EdgeDBManager(BaseDBManager):
    """Manages Edge3 provider database tables."""

    metadata = edge_metadata

    version_table_name = "alembic_version_edge3"
    migration_dir = (PACKAGE_DIR / "migrations").as_posix()
    alembic_file = (PACKAGE_DIR / "alembic.ini").as_posix()
    supports_table_dropping = True
    revision_heads_map = _REVISION_HEADS_MAP

    def initdb(self, use_migration_files: bool = False):
        """
        Initialize the database, handling pre-alembic installations.

        If the edge3 tables already exist but the alembic version table does not
        (e.g. created via create_all before the migration chain was introduced),
        stamp to the first revision and run the incremental upgrade so every
        migration is applied rather than jumping straight to head.
        """
        # Older Airflow's BaseDBManager.upgradedb() does not accept use_migration_files.
        _umf_kwargs: dict = (
            {"use_migration_files": use_migration_files}
            if _callable_accepts_use_migration_files(self.upgradedb)
            else {}
        )

        db_exists = self.get_current_revision()
        if db_exists:
            self.upgradedb(**_umf_kwargs)
        else:
            from airflow import settings

            engine = settings.engine
            inspector: Inspector | None = inspect(engine) if engine is not None else None
            existing_tables = set(inspector.get_table_names()) if inspector is not None else set()
            if any(table in existing_tables for table in self.metadata.tables):
                script = self.get_script_object()
                base_revision = next(r.revision for r in script.walk_revisions() if r.down_revision is None)
                config = self.get_alembic_config()
                from alembic import command

                command.stamp(config, base_revision)
                self.upgradedb(**_umf_kwargs)
            elif use_migration_files:
                self.upgradedb(**_umf_kwargs)
            else:
                self.create_db_from_orm()

    def drop_tables(self, connection):
        """Drop only edge3 tables in reverse dependency order."""
        if not self.supports_table_dropping:
            return

        inspector = inspect(connection)

        edge_tables = [
            EdgeLogsModel.__table__,
            EdgeJobModel.__table__,
            EdgeWorkerModel.__table__,
        ]

        for table in edge_tables:
            if inspector.has_table(table.name):
                self.log.info("Dropping table %s", table.name)
                table.drop(connection)

        version = self._get_migration_ctx()._version
        if inspector.has_table(version.name):
            self.log.info("Dropping version table %s", version.name)
            version.drop(connection)


def check_db_manager_config() -> None:
    """
    Warn if EdgeDBManager is not registered to run DB migrations.

    Should be called whenever the edge3 provider is active so operators are alerted
    early if the required database configuration is missing.
    """
    from airflow.configuration import conf
    from airflow.providers_manager import ProvidersManager

    fqcn = f"{EdgeDBManager.__module__}.{EdgeDBManager.__name__}"

    # Check explicitly configured managers
    configured = conf.get("database", "external_db_managers", fallback="")
    registered = {m.strip() for m in configured.split(",") if m.strip()}
    # Also check auto-discovered managers from installed providers (db_managers added in Airflow 3.2)
    pm = ProvidersManager()
    if hasattr(pm, "db_managers"):
        registered |= set(pm.db_managers)

    if fqcn not in registered:
        warnings.warn(
            f"EdgeDBManager is not configured. Add '{fqcn}' to "
            f"AIRFLOW__DATABASE__EXTERNAL_DB_MANAGERS (the 'external_db_managers' option "
            f"in the [database] section). Without this, edge3 database tables will not be "
            f"managed through the standard Airflow migration process.",
            UserWarning,
            stacklevel=2,
        )
