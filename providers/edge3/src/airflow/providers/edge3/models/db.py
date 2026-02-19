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

from pathlib import Path

from sqlalchemy import MetaData, inspect

from airflow.models.base import Base
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel
from airflow.utils.db_manager import BaseDBManager

PACKAGE_DIR = Path(__file__).parents[1]

_REVISION_HEADS_MAP: dict[str, str] = {
    "3.0.0": "9d34dfc2de06",
}

# Create filtered metadata containing only edge3 tables
# This avoids validation issues with shared Base.metadata
_edge_metadata = MetaData()
EdgeWorkerModel.__table__.to_metadata(_edge_metadata)
EdgeJobModel.__table__.to_metadata(_edge_metadata)
EdgeLogsModel.__table__.to_metadata(_edge_metadata)

# Remove edge tables from Airflow's core metadata to prevent validation conflicts
# The tables are now managed exclusively through _edge_metadata
Base.metadata.remove(EdgeWorkerModel.__table__)
Base.metadata.remove(EdgeJobModel.__table__)
Base.metadata.remove(EdgeLogsModel.__table__)


class EdgeDBManager(BaseDBManager):
    """Manages Edge3 provider database tables."""

    # Use filtered metadata instead of shared Base.metadata
    metadata = _edge_metadata

    version_table_name = "alembic_version_edge3"
    migration_dir = (PACKAGE_DIR / "migrations").as_posix()
    alembic_file = (PACKAGE_DIR / "alembic.ini").as_posix()
    supports_table_dropping = True
    revision_heads_map = _REVISION_HEADS_MAP

    def drop_tables(self, connection):
        """
        Drop only edge3 tables.

        Override base implementation to avoid dropping all tables in shared metadata.
        """
        if not self.supports_table_dropping:
            return

        inspector = inspect(connection)

        # Drop edge3 tables in reverse dependency order
        edge_tables = [
            EdgeLogsModel.__table__,
            EdgeJobModel.__table__,
            EdgeWorkerModel.__table__,
        ]

        for table in edge_tables:
            if inspector.has_table(table.name):
                self.log.info("Dropping table %s", table.name)
                table.drop(connection)

        # Drop version table
        version = self._get_migration_ctx()._version
        if inspector.has_table(version.name):
            self.log.info("Dropping version table %s", version.name)
            version.drop(connection)
