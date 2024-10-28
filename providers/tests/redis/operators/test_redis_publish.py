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

from unittest.mock import MagicMock, patch

from airflow.models.dag import DAG
from airflow.providers.redis.operators.redis_publish import RedisPublishOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestRedisPublishOperator:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}

        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

        self.mock_context = MagicMock()

    @patch("airflow.providers.redis.hooks.redis.RedisHook.get_conn")
    def test_execute_operator(self, mock_redis_conn):
        operator = RedisPublishOperator(
            task_id="test_task",
            dag=self.dag,
            channel="test_channel",
            message="test_message",
            redis_conn_id="redis_default",
        )
        operator.execute(self.mock_context)

        mock_redis_conn.assert_called_once_with()
        mock_redis_conn().publish.assert_called_once_with(
            channel="test_channel", message="test_message"
        )
