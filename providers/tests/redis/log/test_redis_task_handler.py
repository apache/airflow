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
from unittest.mock import patch

import pytest
from tests_common.test_utils.config import conf_vars

from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.providers.redis.log.redis_task_handler import RedisTaskHandler
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime

pytestmark = pytest.mark.db_test


class TestRedisTaskHandler:
    @pytest.fixture
    def ti(self):
        date = datetime(2020, 1, 1)
        dag = DAG(dag_id="dag_for_testing_redis_task_handler", schedule=None, start_date=date)
        task = EmptyOperator(task_id="task_for_testing_redis_log_handler", dag=dag)
        dag_run = DagRun(dag_id=dag.dag_id, execution_date=date, run_id="test", run_type="scheduled")

        with create_session() as session:
            session.add(dag_run)
            session.commit()
            session.refresh(dag_run)

        ti = TaskInstance(task=task, run_id=dag_run.run_id)
        ti.dag_run = dag_run
        ti.try_number = 1
        ti.state = State.RUNNING

        yield ti

        with create_session() as session:
            session.query(DagRun).delete()

    @conf_vars({("logging", "remote_log_conn_id"): "redis_default"})
    def test_write(self, ti):
        handler = RedisTaskHandler("any", max_lines=5, ttl_seconds=2)
        handler.set_context(ti)
        logger = logging.getLogger(__name__)
        logger.addHandler(handler)

        key = (
            "dag_id=dag_for_testing_redis_task_handler/run_id=test"
            "/task_id=task_for_testing_redis_log_handler/attempt=1.log"
        )

        with patch("redis.Redis.pipeline") as pipeline:
            logger.info("Test log event")

        pipeline.return_value.rpush.assert_called_once_with(key, "Test log event")
        pipeline.return_value.ltrim.assert_called_once_with(key, start=-5, end=-1)
        pipeline.return_value.expire.assert_called_once_with(key, time=2)
        pipeline.return_value.execute.assert_called_once_with()

    @conf_vars({("logging", "remote_log_conn_id"): "redis_default"})
    def test_read(self, ti):
        handler = RedisTaskHandler("any")
        handler.set_context(ti)
        logger = logging.getLogger(__name__)
        logger.addHandler(handler)

        key = (
            "dag_id=dag_for_testing_redis_task_handler/run_id=test"
            "/task_id=task_for_testing_redis_log_handler/attempt=1.log"
        )

        with patch("redis.Redis.lrange") as lrange:
            lrange.return_value = [b"Line 1", b"Line 2"]
            logs = handler.read(ti)

        assert logs == ([[("", "Line 1\nLine 2")]], [{"end_of_log": True}])
        lrange.assert_called_once_with(key, start=0, end=-1)
