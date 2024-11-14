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

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DAG
from airflow.providers.apache.kylin.operators.kylin_cube import KylinCubeOperator
from airflow.utils import timezone
from airflow.utils.types import DagRunType

DEFAULT_DATE = timezone.datetime(2020, 1, 1)


class TestKylinCubeOperator:
    _config = {
        "kylin_conn_id": "kylin_default",
        "project": "learn_kylin",
        "cube": "kylin_sales_cube",
        "command": "build",
        "start_time": str(int(datetime(2012, 1, 2, 0, 0).timestamp() * 1000)),
        "end_time": str(int(datetime(2012, 1, 3, 0, 0).timestamp() * 1000)),
    }
    cube_command = [
        "fullbuild",
        "build",
        "merge",
        "refresh",
        "delete",
        "build_streaming",
        "merge_streaming",
        "refresh_streaming",
        "disable",
        "enable",
        "purge",
        "clone",
        "drop",
    ]

    build_response = {"uuid": "c143e0e4-ac5f-434d-acf3-46b0d15e3dc6"}

    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

    @patch("airflow.providers.apache.kylin.operators.kylin_cube.KylinHook")
    def test_execute(self, mock_hook):
        operator = KylinCubeOperator(task_id="kylin_task", dag=self.dag, **self._config)
        hook = MagicMock()
        hook.invoke_command = [
            "fullbuild",
            "build",
            "merge",
            "refresh",
            "delete",
            "build_streaming",
            "merge_streaming",
            "refresh_streaming",
            "disable",
            "enable",
            "purge",
            "clone",
            "drop",
        ]
        mock_hook.return_value = hook
        mock_hook.cube_run.return_value = {}

        assert operator is not None
        assert self._config["kylin_conn_id"] == operator.kylin_conn_id
        assert self._config["project"] == operator.project
        assert self._config["cube"] == operator.cube
        assert self._config["command"] == operator.command
        assert self._config["start_time"] == operator.start_time
        assert self._config["end_time"] == operator.end_time
        operator.execute(None)
        mock_hook.assert_called_once_with(
            kylin_conn_id=self._config["kylin_conn_id"], project=self._config["project"], dsn=None
        )

        mock_hook.return_value.cube_run.assert_called_once_with(
            "kylin_sales_cube",
            "build",
            end=datetime(2012, 1, 3, 0, 0),
            name=None,
            offset_end=None,
            offset_start=None,
            start=datetime(2012, 1, 2, 0, 0),
        )

    @patch("airflow.providers.apache.kylin.operators.kylin_cube.KylinHook")
    def test_execute_build(self, mock_hook):
        operator = KylinCubeOperator(
            is_track_job=True, timeout=5, interval=1, task_id="kylin_task", dag=self.dag, **self._config
        )
        hook = MagicMock()
        hook.invoke_command = self.cube_command
        hook.cube_run.return_value = self.build_response

        hook.get_job_status.side_effect = ["RUNNING", "RUNNING", "FINISHED"]
        mock_hook.return_value = hook
        assert operator.execute(None)["uuid"] == "c143e0e4-ac5f-434d-acf3-46b0d15e3dc6"

    @patch("airflow.providers.apache.kylin.operators.kylin_cube.KylinHook")
    def test_execute_build_status_error(self, mock_hook):
        operator = KylinCubeOperator(
            is_track_job=True, timeout=5, interval=1, task_id="kylin_task", dag=self.dag, **self._config
        )
        hook = MagicMock()
        hook.invoke_command = self.cube_command
        hook.cube_run.return_value = self.build_response

        hook.get_job_status.return_value = "ERROR"
        mock_hook.return_value = hook
        with pytest.raises(AirflowException):
            operator.execute(None)

    @patch("airflow.providers.apache.kylin.operators.kylin_cube.KylinHook")
    def test_execute_build_time_out_error(self, mock_hook):
        operator = KylinCubeOperator(
            is_track_job=True, timeout=5, interval=1, task_id="kylin_task", dag=self.dag, **self._config
        )
        hook = MagicMock()
        hook.invoke_command = self.cube_command
        hook.cube_run.return_value = self.build_response

        hook.get_job_status.return_value = "RUNNING"
        mock_hook.return_value = hook
        with pytest.raises(AirflowException):
            operator.execute(None)

    @pytest.mark.db_test
    def test_render_template(self, session):
        operator = KylinCubeOperator(
            task_id="kylin_build_1",
            kylin_conn_id="kylin_default",
            project="{{ params.project }}",
            cube="{{ params.cube }}",
            command="{{ params.command }}",
            start_time="{{ params.start_time }}",
            end_time="{{ params.end_time }}",
            is_track_job=True,
            dag=self.dag,
            params={
                "project": "learn_kylin",
                "cube": "kylin_sales_cube",
                "command": "build",
                "start_time": "1483200000000",
                "end_time": "1483286400000",
            },
        )
        ti = TaskInstance(operator, run_id="kylin_test")
        ti.dag_run = DagRun(
            dag_id=self.dag.dag_id,
            run_id="kylin_test",
            execution_date=DEFAULT_DATE,
            run_type=DagRunType.MANUAL,
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        assert "learn_kylin" == getattr(operator, "project")
        assert "kylin_sales_cube" == getattr(operator, "cube")
        assert "build" == getattr(operator, "command")
        assert "1483200000000" == getattr(operator, "start_time")
        assert "1483286400000" == getattr(operator, "end_time")
