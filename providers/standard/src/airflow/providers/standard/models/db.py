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

import logging
from pathlib import Path

from airflow.providers.standard.models import metadata
from airflow.providers.standard.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.utils.db_manager import BaseDBManager

PACKAGE_DIR = Path(__file__).parents[1]

_REVISION_HEADS_MAP: dict[str, str] = {
    "1.3.0": "5e7113ca79cc",
}
log = logging.getLogger(__name__)

if not AIRFLOW_V_3_1_PLUS:
    log.warning("Human in the loop functionality needs Airflow 3.1+. Skip loadding HITLDBManager.")
else:

    class HITLDBManager(BaseDBManager):
        """Manages Human in the loop database."""

        metadata = metadata
        version_table_name = "alembic_version_hitl"
        migration_dir = (PACKAGE_DIR / "migrations").as_posix()
        alembic_file = (PACKAGE_DIR / "alembic.ini").as_posix()
        supports_table_dropping = True
        revision_heads_map = _REVISION_HEADS_MAP
