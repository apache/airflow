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

from unittest.mock import patch

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models import DagModel
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import create_session


@pytest.mark.db_test
class TestSchedulerJobFlush:
    @pytest.fixture
    def scheduler_job_runner(self):
        job = Job()
        return SchedulerJobRunner(job=job)

    def test_create_dag_runs_flushes_session(self, dag_maker, scheduler_job_runner):
        # Create 60 DAGs to trigger flush (batch size 50)
        dag_ids = []
        start_date = timezone.datetime(2023, 1, 1)

        # We need to create DAGs and ensure they are persisted
        with create_session() as session:
            for i in range(60):
                dag_id = f"test_flush_dag_{i}"
                dag_ids.append(dag_id)
                with dag_maker(dag_id=dag_id, start_date=start_date, schedule="@daily", session=session) as _:
                    EmptyOperator(task_id="task")

            # Commit to ensure they exist for query later if needed
            session.commit()

        # Re-query dag_models in a new session to pass to the method
        with create_session() as session:
            # Query in order to match our expectations if we iterate
            # But the method takes a collection.
            # Let's query them and sort by dag_id to be deterministic
            test_dag_models = session.scalars(
                select(DagModel).where(DagModel.dag_id.in_(dag_ids)).order_by(DagModel.dag_id)
            ).all()

            # Ensure we have 60
            assert len(test_dag_models) == 60

            # Mock flush and expunge_all
            with (
                patch.object(session, "flush", wraps=session.flush) as mock_flush,
                patch.object(session, "expunge_all", wraps=session.expunge_all) as mock_expunge,
            ):
                scheduler_job_runner._create_dag_runs(test_dag_models, session)

                # Verify flush and expunge_all called at least once (at index 49)
                assert mock_flush.call_count >= 1
                assert mock_expunge.call_count >= 1

                # Check that early items are detached (expunged)
                # Items 0-49 should be detached if expunge happened at 49
                # Items 50-59 should be attached because they were processed after expunge (or added back)
                # But wait, expunge_all clears EVERYTHING.
                # If we process 50-59, we add them to session.
                # So 50-59 should be in session.
                # 0-49 should not be in session.

                # test_dag_models is the list we passed.
                # The objects in the list are the ones we check.

                # Let's check a few
                # Since we sorted by dag_id, we can check by index if consistent
                # But wait, enumerate order depends on list order.
                # test_dag_models is ordered by dag_id.

                # First item (processed early)
                first_dag = test_dag_models[0]
                # Last item (processed late)
                last_dag = test_dag_models[-1]

                def is_in_session(obj, session):
                    try:
                        return obj in session
                    except Exception:
                        return False

                # We expect first_dag to be detached (not in session)
                # Note: `in session` checks if the instance is in the identity map.
                assert first_dag not in session, "First processed DAG should be expunged"

                # Last dag should be in session because it was processed after the flush/expunge
                # (assuming loop finishes without another flush at very end)
                # 60 items. Flush at 50. Items 50-59 processed. No flush at 60 (since (59+1)%50 != 0? Wait.
                # (59+1) = 60. 60 % 50 != 0.
                # So no flush at end.
                assert last_dag in session, "Last processed DAG should be in session"
