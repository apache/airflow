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

from importlib import reload
from unittest import mock

import pytest

from airflow.cli.commands.standalone_command import StandaloneCommand
from airflow.executors import executor_loader
from airflow.executors.executor_constants import (
    CELERY_EXECUTOR,
    CELERY_KUBERNETES_EXECUTOR,
    DEBUG_EXECUTOR,
    KUBERNETES_EXECUTOR,
    LOCAL_EXECUTOR,
    LOCAL_KUBERNETES_EXECUTOR,
    SEQUENTIAL_EXECUTOR,
)


class TestStandaloneCommand:
    @pytest.mark.parametrize(
        "conf_executor_name, conf_sql_alchemy_conn, expected_standalone_executor",
        [
            (LOCAL_EXECUTOR, "sqlite_conn_string", LOCAL_EXECUTOR),
            (LOCAL_KUBERNETES_EXECUTOR, "sqlite_conn_string", SEQUENTIAL_EXECUTOR),
            (SEQUENTIAL_EXECUTOR, "sqlite_conn_string", SEQUENTIAL_EXECUTOR),
            (CELERY_EXECUTOR, "sqlite_conn_string", SEQUENTIAL_EXECUTOR),
            (CELERY_KUBERNETES_EXECUTOR, "sqlite_conn_string", SEQUENTIAL_EXECUTOR),
            (KUBERNETES_EXECUTOR, "sqlite_conn_string", SEQUENTIAL_EXECUTOR),
            (DEBUG_EXECUTOR, "sqlite_conn_string", SEQUENTIAL_EXECUTOR),
            (LOCAL_EXECUTOR, "other_db_conn_string", LOCAL_EXECUTOR),
            (LOCAL_KUBERNETES_EXECUTOR, "other_db_conn_string", LOCAL_EXECUTOR),
            (SEQUENTIAL_EXECUTOR, "other_db_conn_string", SEQUENTIAL_EXECUTOR),
            (CELERY_EXECUTOR, "other_db_conn_string", LOCAL_EXECUTOR),
            (CELERY_KUBERNETES_EXECUTOR, "other_db_conn_string", LOCAL_EXECUTOR),
            (KUBERNETES_EXECUTOR, "other_db_conn_string", LOCAL_EXECUTOR),
            (DEBUG_EXECUTOR, "other_db_conn_string", LOCAL_EXECUTOR),
        ],
    )
    def test_calculate_env(
        self, conf_executor_name, conf_sql_alchemy_conn, expected_standalone_executor
    ):
        """Should always force a local executor compatible with the db."""
        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW__CORE__EXECUTOR": conf_executor_name,
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": conf_sql_alchemy_conn,
            },
        ):
            reload(executor_loader)
            env = StandaloneCommand().calculate_env()
            assert env["AIRFLOW__CORE__EXECUTOR"] == expected_standalone_executor
