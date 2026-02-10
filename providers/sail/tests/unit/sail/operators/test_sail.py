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

from airflow.models.dag import DAG
from airflow.providers.sail.operators.sail import SailPySparkOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2024, 2, 1, tzinfo=timezone.utc)


class TestSailPySparkOperator:
    _config = {
        "conn_id": "sail_special_conn_id",
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

    def test_init_with_conn_id(self):
        def my_spark_fn(spark):
            pass

        operator = SailPySparkOperator(
            task_id="sail_pyspark_job",
            python_callable=my_spark_fn,
            dag=self.dag,
            **self._config,
        )

        assert operator.conn_id == self._config["conn_id"]
        assert operator.config_kwargs == {}
        assert operator.local_server_port == 50051

    def test_init_without_conn_id(self):
        def my_spark_fn(spark):
            pass

        operator = SailPySparkOperator(
            task_id="sail_pyspark_local",
            python_callable=my_spark_fn,
            dag=self.dag,
        )

        assert operator.conn_id is None
        assert operator.config_kwargs == {}

    def test_init_with_config_kwargs(self):
        def my_spark_fn(spark):
            pass

        config = {"spark.executor.memory": "2g"}
        operator = SailPySparkOperator(
            task_id="sail_pyspark_config",
            python_callable=my_spark_fn,
            dag=self.dag,
            config_kwargs=config,
        )

        assert operator.config_kwargs == config

    def test_init_with_custom_port(self):
        def my_spark_fn(spark):
            pass

        operator = SailPySparkOperator(
            task_id="sail_pyspark_port",
            python_callable=my_spark_fn,
            dag=self.dag,
            local_server_port=9999,
        )

        assert operator.local_server_port == 9999

    def test_template_fields(self):
        assert "conn_id" in SailPySparkOperator.template_fields
        assert "config_kwargs" in SailPySparkOperator.template_fields
