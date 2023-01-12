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

from sqlalchemy.orm import Session

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.jobs.scheduler_job import TI
from airflow.utils.retries import run_with_db_retries
from airflow.utils.session import NEW_SESSION, provide_session

logger = logging.getLogger(__name__)


class InternalApiTaskInstanceActions:
    @staticmethod
    @internal_api_call
    @provide_session
    def get_task_instance(
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        lock_for_update: bool = False,
        session: Session = NEW_SESSION,
    ) -> TI | None:
        # TODO: need to convert SQLAlchemy objects to internal API objects
        query = session.query(TI).filter_by(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
        )

        if lock_for_update:
            for attempt in run_with_db_retries(logger=logger):
                with attempt:
                    return query.with_for_update().one_or_none()
        else:
            return query.one_or_none()
