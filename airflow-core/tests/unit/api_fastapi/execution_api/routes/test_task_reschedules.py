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

import pytest

from airflow.models.taskreschedule import TaskReschedule
from airflow.utils.state import State
from airflow.utils.timezone import datetime, parse

DEFAULT_DATE = parse("2021-01-01T00:00:00")

pytestmark = pytest.mark.db_test


class TestGetRescheduleStartDate:
    def test_get_start_date(self, client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_ti_update_state_reschedule_mysql_limit",
            state=State.RUNNING,
            start_date=datetime(2024, 1, 1),
            session=session,
        )
        tr = TaskReschedule(
            task_instance_id=ti.id,
            try_number=1,
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 1, 1),
            reschedule_date=datetime(2024, 1, 1, 2),
        )
        session.add(tr)
        session.commit()

        response = client.get(f"/execution/task-reschedules/{ti.id}/start_date")
        assert response.status_code == 200
        assert response.json() == "2024-01-01T00:00:00Z"

    def test_get_start_date_not_found(self, client):
        ti_id = "0182e924-0f1e-77e6-ab50-e977118bc139"
        response = client.get(f"/execution/task-reschedules/{ti_id}/start_date")
        assert response.status_code == 404

    def test_get_start_date_with_try_number(self, client, session, create_task_instance):
        # Create multiple reschedules
        dates = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3),
        ]

        ti = create_task_instance(
            task_id="test_get_start_date_with_try_number",
            state=State.RUNNING,
            start_date=datetime(2024, 1, 1),
            session=session,
        )

        for i, date in enumerate(dates, 1):
            tr = TaskReschedule(
                task_instance_id=ti.id,
                try_number=i,
                start_date=date,
                end_date=date.replace(hour=1),
                reschedule_date=date.replace(hour=2),
            )
            session.add(tr)
        session.commit()

        # Test getting start date for try_number 2
        response = client.get(f"/execution/task-reschedules/{ti.id}/start_date?try_number=2")
        assert response.status_code == 200
        assert response.json() == "2024-01-02T00:00:00Z"
