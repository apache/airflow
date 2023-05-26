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

from datetime import datetime
from unittest.mock import MagicMock

from airflow.api.common.airflow_health import (
    HEALTHY,
    UNHEALTHY,
    SchedulerJobRunner,
    TriggererJobRunner,
    get_airflow_health,
)


def test_get_airflow_health_only_metadatabase_healthy():
    SchedulerJobRunner.most_recent_job = MagicMock(return_value=None)
    TriggererJobRunner.most_recent_job = MagicMock(return_value=None)

    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {"status": UNHEALTHY, "latest_scheduler_heartbeat": None},
        "triggerer": {"status": None, "latest_triggerer_heartbeat": None},
    }

    assert health_status == expected_status


def test_get_airflow_health_metadatabase_unhealthy():
    SchedulerJobRunner.most_recent_job = MagicMock(side_effect=Exception)
    TriggererJobRunner.most_recent_job = MagicMock(side_effect=Exception)

    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": UNHEALTHY},
        "scheduler": {"status": UNHEALTHY, "latest_scheduler_heartbeat": None},
        "triggerer": {"status": UNHEALTHY, "latest_triggerer_heartbeat": None},
    }

    assert health_status == expected_status


def test_get_airflow_health_scheduler_healthy_no_triggerer():
    latest_scheduler_job_mock = MagicMock()
    latest_scheduler_job_mock.latest_heartbeat = datetime.now()
    latest_scheduler_job_mock.is_alive = MagicMock(return_value=True)
    SchedulerJobRunner.most_recent_job = MagicMock(return_value=latest_scheduler_job_mock)
    TriggererJobRunner.most_recent_job = MagicMock(return_value=None)

    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {
            "status": HEALTHY,
            "latest_scheduler_heartbeat": latest_scheduler_job_mock.latest_heartbeat.isoformat(),
        },
        "triggerer": {"status": None, "latest_triggerer_heartbeat": None},
    }

    assert health_status == expected_status


def test_get_airflow_health_triggerer_healthy_no_scheduler_job_record():
    latest_triggerer_job_mock = MagicMock()
    latest_triggerer_job_mock.latest_heartbeat = datetime.now()
    latest_triggerer_job_mock.is_alive = MagicMock(return_value=True)
    SchedulerJobRunner.most_recent_job = MagicMock(return_value=None)
    TriggererJobRunner.most_recent_job = MagicMock(return_value=latest_triggerer_job_mock)

    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {"status": UNHEALTHY, "latest_scheduler_heartbeat": None},
        "triggerer": {
            "status": HEALTHY,
            "latest_triggerer_heartbeat": latest_triggerer_job_mock.latest_heartbeat.isoformat(),
        },
    }

    assert health_status == expected_status
