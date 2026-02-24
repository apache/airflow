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

from sqlalchemy import inspect

from airflow.providers.edge3.models.edge_base import edge_metadata
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel
from airflow.utils.db_manager import BaseDBManager

PACKAGE_DIR = Path(__file__).parents[1]

_REVISION_HEADS_MAP: dict[str, str] = {
    "3.0.0": "9d34dfc2de06",
}


class EdgeDBManager(BaseDBManager):
    """Manages Edge3 provider database tables."""

    metadata = edge_metadata

    version_table_name = "alembic_version_edge3"
    migration_dir = (PACKAGE_DIR / "migrations").as_posix()
    alembic_file = (PACKAGE_DIR / "alembic.ini").as_posix()
    supports_table_dropping = True
    revision_heads_map = _REVISION_HEADS_MAP

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
    # Also check auto-discovered managers from installed providers
    registered |= set(ProvidersManager().db_managers)

    if fqcn not in registered:
        warnings.warn(
            f"EdgeDBManager is not configured. Add '{fqcn}' to "
            f"AIRFLOW__DATABASE__EXTERNAL_DB_MANAGERS (the 'external_db_managers' option "
            f"in the [database] section). Without this, edge3 database tables will not be "
            f"managed through the standard Airflow migration process.",
            UserWarning,
            stacklevel=2,
        )
