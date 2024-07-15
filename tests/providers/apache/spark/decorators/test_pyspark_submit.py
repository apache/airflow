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

import re
from io import StringIO
from typing import Any
from unittest.mock import patch

import pytest

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.apache.spark.decorators.pyspark_submit import INPUT_FILENAME
from airflow.utils import db
from tests.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestPysparkSubmitDecorator:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="pyspark_local",
                conn_type="spark",
                host="spark://none",
                extra="",
            )
        )

    @patch("airflow.providers.apache.spark.decorators.pyspark_submit.write_pyspark_script")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_pyspark_submit_decorator_with_simple_func(
        self, mock_popen, mock_write_pyspark_script, dag_maker
    ):
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0

        @task.pyspark_submit(conn_id="pyspark_local")
        def some_task_function():
            pass

        with dag_maker():
            ret = some_task_function()

        dr = dag_maker.create_dagrun()
        operator = ret.operator
        operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        cmd_list = mock_popen.call_args[0][0]
        cmd = " ".join(cmd_list)
        assert "spark-submit" in cmd
        assert "--master spark://none" in cmd
        assert re.search(r"/tmp/.*/script.py", cmd) is not None

        mock_write_pyspark_script.assert_called_once()
        jinja_context = mock_write_pyspark_script.call_args[1]["jinja_context"]
        assert jinja_context["use_arguments"] is False
        assert jinja_context["use_spark_context"] is False
        assert jinja_context["use_spark_session"] is False
        assert jinja_context["pickling_library"] == "pickle"
        assert jinja_context["python_callable"] == "some_task_function"
        assert ("def some_task_function():\n    pass\n") in jinja_context["python_callable_source"]
        assert jinja_context["expect_airflow"] is False

    @patch("airflow.providers.apache.spark.decorators.pyspark_submit.write_pyspark_script")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_pyspark_submit_decorator_with_complex_function(self, mock_popen, mock_write_pyspark_script):
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0

        @task.pyspark_submit(conn_id="pyspark_local")
        def some_task_function(a: int, b: str, c: Any = None) -> tuple[int, str, Any]:
            """hello world"""
            return a, b, c

        ret = some_task_function(1, "2", {"c": 3})

        operator = ret.operator
        operator.execute(None)
        assert operator.files
        assert re.search(f"/tmp/.*/{INPUT_FILENAME}", operator.files) is not None

        mock_write_pyspark_script.assert_called_once()
        assert operator.application == mock_write_pyspark_script.call_args[1]["filename"]
        jinja_context = mock_write_pyspark_script.call_args[1]["jinja_context"]
        assert jinja_context["use_arguments"] is True
        assert jinja_context["use_spark_context"] is False
        assert jinja_context["use_spark_session"] is False
        assert re.search(f"/tmp/.*/{INPUT_FILENAME}", jinja_context["input_filename"]) is not None
        assert jinja_context["pickling_library"] == "pickle"
        assert jinja_context["python_callable"] == "some_task_function"
        assert jinja_context["python_callable_source"].startswith(
            "def some_task_function(a: int, b: str, c: Any = None) -> tuple[int, str, Any]:"
        )
        assert jinja_context["expect_airflow"] is False

    @patch("airflow.providers.apache.spark.decorators.pyspark_submit.write_pyspark_script")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_pyspark_submit_decorator_with_op_args_and_op_kwargs(self, mock_popen, mock_write_pyspark_script):
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0

        @task.pyspark_submit(conn_id="pyspark_local")
        def some_task_function(a: int, b: str, c: Any = None) -> tuple[int, str, Any]:
            return a, b, c

        ret = some_task_function(1, "2", {"c": 3})

        operator = ret.operator
        operator.execute(None)

        jinja_context = mock_write_pyspark_script.call_args[1]["jinja_context"]
        assert jinja_context["use_arguments"] is True

    @patch("airflow.providers.apache.spark.decorators.pyspark_submit.write_pyspark_script")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_pyspark_submit_decorator_with_spark_params(self, mock_popen, mock_write_pyspark_script):
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0

        @task.pyspark_submit(conn_id="pyspark_local")
        def f(spark):
            print("spark", spark)

        ret = f()
        operator = ret.operator
        operator.execute(None)

        jinja_context = mock_write_pyspark_script.call_args[1]["jinja_context"]
        assert jinja_context["use_spark_context"] is False
        assert jinja_context["use_spark_session"] is True

    @patch("airflow.providers.apache.spark.decorators.pyspark_submit.write_pyspark_script")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_pyspark_submit_decorator_with_sc_params(self, mock_popen, mock_write_pyspark_script):
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0

        @task.pyspark_submit(conn_id="pyspark_local")
        def f(sc):
            print("sc", sc)

        ret = f()
        operator = ret.operator
        operator.execute(None)

        jinja_context = mock_write_pyspark_script.call_args[1]["jinja_context"]
        assert jinja_context["use_spark_context"] is True
        assert jinja_context["use_spark_session"] is False

    @patch("airflow.providers.apache.spark.decorators.pyspark_submit.write_pyspark_script")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_pyspark_submit_decorator_with_sc_and_spark(self, mock_popen, mock_write_pyspark_script):
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0

        @task.pyspark_submit(conn_id="pyspark_local")
        def f(sc, spark):
            print("sc", sc)
            print("spark", spark)

        ret = f()
        operator = ret.operator
        operator.execute(None)

        jinja_context = mock_write_pyspark_script.call_args[1]["jinja_context"]
        assert jinja_context["use_spark_context"] is True
        assert jinja_context["use_spark_session"] is True

    @conf_vars({("operators", "ALLOW_ILLEGAL_ARGUMENTS"): "false"})
    def test_pyspark_submit_decorator_with_application_decorator_param(self):
        @task.pyspark_submit(conn_id="pyspark_local", application="app.py")
        def f():
            pass

        with pytest.raises(AirflowException) as ctx:
            f()
        assert "Invalid argument 'application' were passed to `@task.pyspark_submit`." in str(ctx.value)

    @conf_vars({("operators", "ALLOW_ILLEGAL_ARGUMENTS"): "false"})
    def test_pyspark_submit_decorator_with_application_args_decorator_param(self):
        @task.pyspark_submit(conn_id="pyspark_local", application_args=["some", "args"])
        def f():
            pass

        with pytest.raises(AirflowException) as ctx:
            f()
        assert "Invalid argument 'application_args' were passed to `@task.pyspark_submit`." in str(ctx.value)

    @conf_vars({("operators", "ALLOW_ILLEGAL_ARGUMENTS"): "false"})
    def test_pyspark_submit_decorator_when_spark_in_op_kwargs(self):
        @task.pyspark_submit(conn_id="pyspark_local")
        def f(spark):
            print(spark)

        with pytest.raises(AirflowException) as ctx:
            f(spark=1)
        assert "Invalid key 'spark' in op_kwargs." in str(ctx.value)

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_pyspark_submit_decorator_with_spark_submit_operator_args(self, mock_popen, dag_maker):
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0

        @task.pyspark_submit(
            conn_id="pyspark_local",
            num_executors=3,
            executor_cores=3,
            executor_memory="3g",
            driver_memory="3g",
        )
        def f():
            pass

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        operator = ret.operator
        operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        cmd_list = mock_popen.call_args[0][0]
        cmd = " ".join(cmd_list)
        assert "--num-executors 3" in cmd
        assert "--executor-cores 3" in cmd
        assert "--executor-memory 3g" in cmd
        assert "--driver-memory 3g" in cmd
