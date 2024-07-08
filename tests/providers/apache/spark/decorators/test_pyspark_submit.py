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
from textwrap import dedent
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

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_simple_pyspark_submit_decorator(self, mock_popen, dag_maker):
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0

        @task.pyspark_submit(conn_id="pyspark_local")
        def f():
            pass

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        operator = ret.operator
        operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        cmd_list = mock_popen.call_args[0][0]
        cmd = " ".join(cmd_list)
        assert "spark-submit" in cmd
        assert "--master spark://none" in cmd
        assert re.search(r"/tmp/.*/script.py", cmd) is not None
        pyspark_source = operator.get_pyspark_source()
        assert "from pyspark import SparkFiles" in pyspark_source
        assert "from pyspark.sql import SparkSession" in pyspark_source

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_pyspark_submit_decorator_with_op_args_and_op_kwargs(self, mock_popen):
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0

        @task.pyspark_submit(conn_id="pyspark_local")
        def f(a: int, b: str, c: dict = None):
            return a, b, c

        ret = f(1, "2", {"c": 3})

        operator = ret.operator
        with patch.object(
            operator, "get_pyspark_source", wraps=operator.get_pyspark_source
        ) as mock_get_pyspark_source:
            operator.execute(None)
            mock_get_pyspark_source.assert_called_once()
            pyspark_source = mock_get_pyspark_source()
            argument_block = dedent(
                f"""\
                if True:
                    SparkSession.builder.getOrCreate()
                    with open(SparkFiles.get("{INPUT_FILENAME}"), "rb") as file:
                        arg_dict = pickle.load(file)"""
            )
            assert argument_block in pyspark_source
            assert operator.files
            assert re.search(f"/tmp/.*/{INPUT_FILENAME}", operator.files) is not None

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

    def test_pyspark_submit_decorator_with_spark_params(self):
        @task.pyspark_submit(conn_id="pyspark_local")
        def f(spark):
            print(spark)

        ret = f()
        operator = ret.operator
        pyspark_source = operator.get_pyspark_source()
        use_spark_session_block = dedent(
            """\
            if True:
                arg_dict["kwargs"]["spark"] = SparkSession.builder.getOrCreate()"""
        )
        assert use_spark_session_block in pyspark_source

    def test_pyspark_submit_decorator_with_sc_params(self):
        @task.pyspark_submit(conn_id="pyspark_local")
        def f(sc):
            pass

        ret = f()
        operator = ret.operator
        pyspark_source = operator.get_pyspark_source()
        use_spark_context_block = (
            "if True:\n"
            '    spark = arg_dict.get("spark") or SparkSession.builder.getOrCreate()\n'
            '    arg_dict["kwargs"]["sc"] = spark.sparkContext'
        )
        assert use_spark_context_block in pyspark_source

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
