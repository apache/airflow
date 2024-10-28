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
from unittest.mock import MagicMock, patch

import pytest

from airflow.api.common.airflow_health import (
    HEALTHY,
    UNHEALTHY,
    get_airflow_health,
)

pytestmark = pytest.mark.db_test


@patch(
    "airflow.api.common.airflow_health.SchedulerJobRunner.most_recent_job",
    return_value=None,
)
@patch(
    "airflow.api.common.airflow_health.TriggererJobRunner.most_recent_job",
    return_value=None,
)
@patch(
    "airflow.api.common.airflow_health.DagProcessorJobRunner.most_recent_job",
    return_value=None,
)
def test_get_airflow_health_only_metadatabase_healthy(
    latest_scheduler_job_mock, latest_triggerer_job_mock, latest_dag_processor_job_mock
):
    health_status = get_airflow_health()
    expected_status = {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {"status": UNHEALTHY, "latest_scheduler_heartbeat": None},
        "triggerer": {"status": None, "latest_triggerer_heartbeat": None},
        "dag_processor": {"status": None, "latest_dag_processor_heartbeat": None},
    }

    assert health_status == expected_status


@patch(
    "airflow.api.common.airflow_health.SchedulerJobRunner.most_recent_job",
    return_value=Exception,
)
@patch(
    "airflow.api.common.airflow_health.TriggererJobRunner.most_recent_job",
    return_value=Exception,
)
@patch(
    "airflow.api.common.airflow_health.DagProcessorJobRunner.most_recent_job",
    return_value=Exception,
)
def test_get_airflow_health_metadatabase_unhealthy(
    latest_scheduler_job_mock, latest_triggerer_job_mock, latest_dag_processor_job_mock
):
    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": UNHEALTHY},
        "scheduler": {"status": UNHEALTHY, "latest_scheduler_heartbeat": None},
        "triggerer": {"status": UNHEALTHY, "latest_triggerer_heartbeat": None},
        "dag_processor": {"status": UNHEALTHY, "latest_dag_processor_heartbeat": None},
    }

    assert health_status == expected_status


LATEST_SCHEDULER_JOB_MOCK = MagicMock()
LATEST_SCHEDULER_JOB_MOCK.latest_heartbeat = datetime.now()
LATEST_SCHEDULER_JOB_MOCK.is_alive = MagicMock(return_value=True)


@patch(
    "airflow.api.common.airflow_health.SchedulerJobRunner.most_recent_job",
    return_value=LATEST_SCHEDULER_JOB_MOCK,
)
@patch(
    "airflow.api.common.airflow_health.TriggererJobRunner.most_recent_job",
    return_value=None,
)
@patch(
    "airflow.api.common.airflow_health.DagProcessorJobRunner.most_recent_job",
    return_value=None,
)
def test_get_airflow_health_scheduler_healthy_no_triggerer(
    latest_scheduler_job_mock, latest_triggerer_job_mock, latest_dag_processor_job_mock
):
    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {
            "status": HEALTHY,
            "latest_scheduler_heartbeat": LATEST_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
        },
        "triggerer": {"status": None, "latest_triggerer_heartbeat": None},
        "dag_processor": {"status": None, "latest_dag_processor_heartbeat": None},
    }

    assert health_status == expected_status


LATEST_TRIGGERER_JOB_MOCK = MagicMock()
LATEST_TRIGGERER_JOB_MOCK.latest_heartbeat = datetime.now()
LATEST_TRIGGERER_JOB_MOCK.is_alive = MagicMock(return_value=True)

LATEST_DAG_PROCESSOR_JOB_MOCK = MagicMock()
LATEST_DAG_PROCESSOR_JOB_MOCK.latest_heartbeat = datetime.now()
LATEST_DAG_PROCESSOR_JOB_MOCK.is_alive = MagicMock(return_value=True)


@patch(
    "airflow.api.common.airflow_health.SchedulerJobRunner.most_recent_job",
    return_value=None,
)
@patch(
    "airflow.api.common.airflow_health.TriggererJobRunner.most_recent_job",
    return_value=LATEST_TRIGGERER_JOB_MOCK,
)
@patch(
    "airflow.api.common.airflow_health.DagProcessorJobRunner.most_recent_job",
    return_value=LATEST_DAG_PROCESSOR_JOB_MOCK,
)
def test_get_airflow_health_triggerer_healthy_no_scheduler_job_record(
    latest_scheduler_job_mock, latest_triggerer_job_mock, latest_dag_processor_job_mock
):
    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {"status": UNHEALTHY, "latest_scheduler_heartbeat": None},
        "triggerer": {
            "status": HEALTHY,
            "latest_triggerer_heartbeat": LATEST_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
        },
        "dag_processor": {
            "status": HEALTHY,
            "latest_dag_processor_heartbeat": LATEST_DAG_PROCESSOR_JOB_MOCK.latest_heartbeat.isoformat(),
        },
    }

    assert health_status == expected_status
