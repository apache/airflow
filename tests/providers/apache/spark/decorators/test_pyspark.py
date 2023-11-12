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

from unittest import mock

import pytest

from airflow.decorators import task
from airflow.models import Connection
from airflow.utils import db, timezone

DEFAULT_DATE = timezone.datetime(2021, 9, 1)


class TestPysparkDecorator:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="pyspark_local",
                conn_type="spark",
                host="spark://none",
                extra="",
            )
        )

    @pytest.mark.db_test
    @mock.patch("pyspark.SparkConf.setAppName")
    @mock.patch("pyspark.sql.SparkSession")
    def test_pyspark_decorator_with_connection(self, spark_mock, conf_mock, dag_maker):
        @task.pyspark(conn_id="pyspark_local", config_kwargs={"spark.executor.memory": "2g"})
        def f(spark, sc):
            import random

            return [random.random() for _ in range(100)]

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert len(ti.xcom_pull()) == 100
        conf_mock().set.assert_called_with("spark.executor.memory", "2g")
        conf_mock().setMaster.assert_called_once_with("spark://none")
        spark_mock.builder.config.assert_called_once_with(conf=conf_mock())

    @pytest.mark.db_test
    @mock.patch("pyspark.SparkConf.setAppName")
    @mock.patch("pyspark.sql.SparkSession")
    def test_simple_pyspark_decorator(self, spark_mock, conf_mock, dag_maker):
        e = 2

        @task.pyspark
        def f():
            return e

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == e
        conf_mock().setMaster.assert_called_once_with("local[*]")
        spark_mock.builder.config.assert_called_once_with(conf=conf_mock())
