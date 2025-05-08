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

from datetime import timedelta

from airflow.configuration import conf
from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.models.dag import DagModel
from airflow.utils import timezone
from airflow.utils.session import create_session
from tests.test_utils.db import clear_db_dags


class TestStaleDagCleanup:
    """Test that stale DAGs get deactivated based on raw dag_directory path"""

    def test_deactivate_stale_dags(self):
        threshold = conf.getint("scheduler", "stale_dag_threshold")
        now = timezone.utcnow()

        stale_time = now - timedelta(seconds=threshold + 5)
        fresh_time = now - timedelta(seconds=threshold - 5)

        clear_db_dags()
        with create_session() as session:
            dm_stale = DagModel(
                dag_id="dag_stale",
                fileloc="/link/dag_stale.py",
                is_active=True,
                last_parsed_time=stale_time,
            )
            dm_fresh = DagModel(
                dag_id="dag_fresh",
                fileloc="/link/dag_fresh.py",
                is_active=True,
                last_parsed_time=fresh_time,
            )
            session.add_all([dm_stale, dm_fresh])
            session.commit()

            last_parsed = {
                "/link/dag_stale.py": now,
                "/link/dag_fresh.py": now,
            }
            DagFileProcessorManager.deactivate_stale_dags(
                last_parsed=last_parsed,
                dag_directory="/link",
                stale_dag_threshold=threshold,
                session=session,
            )
            session.commit()

            ref1 = session.get(DagModel, "dag_stale")
            ref2 = session.get(DagModel, "dag_fresh")
            assert not ref1.is_active, "dag_stale should be deactivated as stale"
            assert ref2.is_active, "dag_fresh should remain active as fresh"
