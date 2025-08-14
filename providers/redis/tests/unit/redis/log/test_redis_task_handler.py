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

from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.redis.log.redis_task_handler import RedisTaskHandler
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs
from tests_common.test_utils.file_task_handler import extract_events
from tests_common.test_utils.version_compat import (
    AIRFLOW_V_3_0_PLUS,
    get_base_airflow_version_tuple,
)


class TestRedisTaskHandler:
    @staticmethod
    def clear_db():
        clear_db_dags()
        clear_db_runs()
        if AIRFLOW_V_3_0_PLUS:
            clear_db_dag_bundles()

    @pytest.fixture
    def ti(self):
        date = datetime(2020, 1, 1)
        dag = DAG(dag_id="dag_for_testing_redis_task_handler", schedule=None, start_date=date)
        task = EmptyOperator(task_id="task_for_testing_redis_log_handler", dag=dag)
        if AIRFLOW_V_3_0_PLUS:
            dag_run = DagRun(
                dag_id=dag.dag_id,
                logical_date=date,
                data_interval=(date, date),
                run_after=date,
                run_id="test",
                run_type="scheduled",
            )
        else:
            dag_run = DagRun(
                dag_id=dag.dag_id,
                execution_date=date,
                run_id="test",
                run_type="scheduled",
            )

        dag_run.set_state(State.RUNNING)
        with create_session() as session:
            session.add(dag_run)
            session.commit()
            session.refresh(dag_run)

        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion
            from airflow.models.dagbundle import DagBundleModel

            bundle_name = "testing"
            with create_session() as session:
                orm_dag_bundle = DagBundleModel(name=bundle_name)
                session.add(orm_dag_bundle)
                session.commit()
            DAG.bulk_write_to_db(bundle_name, None, [dag])
            SerializedDagModel.write_dag(dag, bundle_name=bundle_name)
            dag_version = DagVersion.get_latest_version(dag.dag_id)
            ti = TaskInstance(task=task, run_id=dag_run.run_id, dag_version_id=dag_version.id)
        else:
            ti = TaskInstance(task=task, run_id=dag_run.run_id)
        ti.dag_run = dag_run
        ti.try_number = 1
        ti.state = State.RUNNING

        yield ti

        self.clear_db()

    @pytest.mark.db_test
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

    @pytest.mark.db_test
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

        if AIRFLOW_V_3_0_PLUS:
            if get_base_airflow_version_tuple() < (3, 0, 4):
                assert logs == (["Line 1\nLine 2"], {"end_of_log": True})
            else:
                log_stream, metadata = logs
                assert extract_events(log_stream) == ["Line 1", "Line 2"]
                assert metadata == {"end_of_log": True}
        else:
            assert logs == ([[("", "Line 1\nLine 2")]], [{"end_of_log": True}])
        lrange.assert_called_once_with(key, start=0, end=-1)
