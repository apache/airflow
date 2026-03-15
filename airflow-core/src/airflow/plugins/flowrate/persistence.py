#
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
from datetime import datetime
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.models.flowrate_metric import FlowRateMetric
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)


def _is_flowrate_enabled() -> bool:
    try:
        return conf.getboolean("flowrate", "enabled")
    except Exception:
        log.debug("FlowRate configuration not found; treating as disabled.", exc_info=True)
        return False


@provide_session
def save_task_metric(
    dag_id: str,
    run_id: str,
    task_id: str,
    start_date: datetime | None,
    end_date: datetime | None,
    cpu_request: float | None = None,
    memory_request: float | None = None,
    estimated_cost: float | None = None,
    *,
    session: Session = NEW_SESSION,
) -> None:
    """
    Persist a single FlowRate task metric record. This function is a
    no-op if FlowRate is disabled in configuration, and will
    catch and log all exceptions instead of raising.
    """
    if not _is_flowrate_enabled():
        return

    try:
        metric = FlowRateMetric(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            start_date=start_date,
            end_date=end_date,
            cpu_request=cpu_request,
            memory_request=memory_request,
            estimated_cost=estimated_cost,
        )
        session.add(metric)
        session.flush()
    except Exception:
        log.exception(
            "Failed to persist FlowRate metric for dag_id=%s, run_id=%s, task_id=%s",
            dag_id,
            run_id,
            task_id,
        )

