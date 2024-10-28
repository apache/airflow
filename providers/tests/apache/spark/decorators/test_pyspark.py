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

from typing import Any
from unittest import mock

import pytest

from airflow.decorators import task
from airflow.models import Connection
from airflow.utils import db, timezone

DEFAULT_DATE = timezone.datetime(2021, 9, 1)


pytestmark = pytest.mark.need_serialized_dag


class FakeConfig:
    data: dict[str, Any]

    def __init__(self, data: dict[str, Any] | None = None):
        if data:
            self.data = data
        else:
            self.data = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value


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

        db.merge_conn(
            Connection(
                conn_id="spark-connect",
                conn_type="spark",
                host="sc://localhost",
                extra="",
            )
        )

        db.merge_conn(
            Connection(
                conn_id="spark-connect-auth",
                conn_type="spark_connect",
                host="sc://localhost",
                password="1234",
                login="connect",
                extra={
                    "use_ssl": True,
                },
            )
        )

    @pytest.mark.db_test
    @mock.patch("pyspark.SparkConf")
    @mock.patch("pyspark.sql.SparkSession")
    def test_pyspark_decorator_with_connection(self, spark_mock, conf_mock, dag_maker):
        config = FakeConfig()

        builder = mock.MagicMock()
        spark_mock.builder.config.return_value = builder
        builder.getOrCreate.return_value = builder
        builder.sparkContext.return_value = builder

        conf_mock.return_value = config

        @task.pyspark(
            conn_id="pyspark_local", config_kwargs={"spark.executor.memory": "2g"}
        )
        def f(spark, sc):
            import random

            assert spark is not None
            assert sc is not None
            return [random.random() for _ in range(100)]

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert len(ti.xcom_pull()) == 100
        assert config.get("spark.master") == "spark://none"
        assert config.get("spark.executor.memory") == "2g"
        assert config.get("spark.remote") is None
        assert config.get("spark.app.name")

        spark_mock.builder.config.assert_called_once_with(conf=conf_mock())

    @pytest.mark.db_test
    @mock.patch("pyspark.SparkConf")
    @mock.patch("pyspark.sql.SparkSession")
    def test_simple_pyspark_decorator(self, spark_mock, conf_mock, dag_maker):
        config = FakeConfig()
        conf_mock.return_value = config

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
        assert config.get("spark.master") == "local[*]"
        spark_mock.builder.config.assert_called_once_with(conf=conf_mock())

    @pytest.mark.db_test
    @mock.patch("pyspark.SparkConf")
    @mock.patch("pyspark.sql.SparkSession")
    def test_spark_connect(self, spark_mock, conf_mock, dag_maker):
        config = FakeConfig()
        conf_mock.return_value = config

        @task.pyspark(conn_id="spark-connect")
        def f(spark, sc):
            assert spark is not None
            assert sc is None

            return True

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull()
        assert config.get("spark.remote") == "sc://localhost"
        assert config.get("spark.master") is None
        assert config.get("spark.app.name")
        spark_mock.builder.config.assert_called_once_with(conf=conf_mock())

    @pytest.mark.db_test
    @mock.patch("pyspark.SparkConf")
    @mock.patch("pyspark.sql.SparkSession")
    def test_spark_connect_auth(self, spark_mock, conf_mock, dag_maker):
        config = FakeConfig()
        conf_mock.return_value = config

        @task.pyspark(conn_id="spark-connect-auth")
        def f(spark, sc):
            assert spark is not None
            assert sc is None

            return True

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull()
        assert (
            config.get("spark.remote")
            == "sc://localhost/;user_id=connect;token=1234;use_ssl=True"
        )
        assert config.get("spark.master") is None
        assert config.get("spark.app.name")
