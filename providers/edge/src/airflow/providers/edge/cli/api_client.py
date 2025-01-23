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

import json
import logging
import os
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urljoin

import requests
from retryhttp import retry, wait_retry_after
from tenacity import before_log, wait_random_exponential

from airflow.configuration import conf
from airflow.providers.edge.worker_api.auth import jwt_signer
from airflow.providers.edge.worker_api.datamodels import (
    EdgeJobFetched,
    PushLogsBody,
    WorkerQueuesBody,
    WorkerStateBody,
)
from airflow.utils.state import TaskInstanceState  # noqa: TC001

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.providers.edge.models.edge_worker import EdgeWorkerState

logger = logging.getLogger(__name__)


# Hidden config options for Edge Worker how retries on HTTP requests should be handled
# Note: Given defaults make attempts after 1, 3, 7, 15, 31seconds, 1:03, 2:07, 3:37 and fails after 5:07min
# So far there is no other config facility in Task SDK we use ENV for the moment
# TODO: Consider these env variables jointly in task sdk together with task_sdk/src/airflow/sdk/api/client.py
API_RETRIES = int(os.getenv("AIRFLOW__EDGE__API_RETRIES", os.getenv("AIRFLOW__WORKERS__API_RETRIES", 10)))
API_RETRY_WAIT_MIN = float(
    os.getenv("AIRFLOW__EDGE__API_RETRY_WAIT_MIN", os.getenv("AIRFLOW__WORKERS__API_RETRY_WAIT_MIN", 1.0))
)
API_RETRY_WAIT_MAX = float(
    os.getenv("AIRFLOW__EDGE__API_RETRY_WAIT_MAX", os.getenv("AIRFLOW__WORKERS__API_RETRY_WAIT_MAX", 90.0))
)


_default_wait = wait_random_exponential(min=API_RETRY_WAIT_MIN, max=API_RETRY_WAIT_MAX)


@retry(
    reraise=True,
    max_attempt_number=API_RETRIES,
    wait_server_errors=_default_wait,
    wait_network_errors=_default_wait,
    wait_timeouts=_default_wait,
    wait_rate_limited=wait_retry_after(fallback=_default_wait),  # No infinite timeout on HTTP 429
    before_sleep=before_log(logger, logging.WARNING),
)
def _make_generic_request(method: str, rest_path: str, data: str | None = None) -> Any:
    signer = jwt_signer()
    api_url = conf.get("edge", "api_url")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": signer.generate_signed_token({"method": rest_path}),
    }
    api_endpoint = urljoin(api_url, rest_path)
    response = requests.request(method, url=api_endpoint, data=data, headers=headers)
    response.raise_for_status()
    if response.status_code == HTTPStatus.NO_CONTENT:
        return None
    return json.loads(response.content)


def worker_register(
    hostname: str, state: EdgeWorkerState, queues: list[str] | None, sysinfo: dict
) -> datetime:
    """Register worker with the Edge API."""
    result = _make_generic_request(
        "POST",
        f"worker/{quote(hostname)}",
        WorkerStateBody(state=state, jobs_active=0, queues=queues, sysinfo=sysinfo).model_dump_json(
            exclude_unset=True
        ),
    )
    return datetime.fromisoformat(result)


def worker_set_state(
    hostname: str, state: EdgeWorkerState, jobs_active: int, queues: list[str] | None, sysinfo: dict
) -> list[str] | None:
    """Update the state of the worker in the central site and thereby implicitly heartbeat."""
    return _make_generic_request(
        "PATCH",
        f"worker/{quote(hostname)}",
        WorkerStateBody(state=state, jobs_active=jobs_active, queues=queues, sysinfo=sysinfo).model_dump_json(
            exclude_unset=True
        ),
    )


def jobs_fetch(hostname: str, queues: list[str] | None, free_concurrency: int) -> EdgeJobFetched | None:
    """Fetch a job to execute on the edge worker."""
    result = _make_generic_request(
        "POST",
        f"jobs/fetch/{quote(hostname)}",
        WorkerQueuesBody(queues=queues, free_concurrency=free_concurrency).model_dump_json(
            exclude_unset=True
        ),
    )
    if result:
        return EdgeJobFetched(**result)
    return None


def jobs_set_state(key: TaskInstanceKey, state: TaskInstanceState) -> None:
    """Set the state of a job."""
    _make_generic_request(
        "PATCH",
        f"jobs/state/{key.dag_id}/{key.task_id}/{key.run_id}/{key.try_number}/{key.map_index}/{state}",
    )


def logs_logfile_path(task: TaskInstanceKey) -> Path:
    """Elaborate the path and filename to expect from task execution."""
    result = _make_generic_request(
        "GET",
        f"logs/logfile_path/{task.dag_id}/{task.task_id}/{task.run_id}/{task.try_number}/{task.map_index}",
    )
    base_log_folder = conf.get("logging", "base_log_folder", fallback="NOT AVAILABLE")
    return Path(base_log_folder, result)


def logs_push(
    task: TaskInstanceKey,
    log_chunk_time: datetime,
    log_chunk_data: str,
) -> None:
    """Push an incremental log chunk from Edge Worker to central site."""
    _make_generic_request(
        "POST",
        f"logs/push/{task.dag_id}/{task.task_id}/{task.run_id}/{task.try_number}/{task.map_index}",
        PushLogsBody(log_chunk_time=log_chunk_time, log_chunk_data=log_chunk_data).model_dump_json(
            exclude_unset=True
        ),
    )
