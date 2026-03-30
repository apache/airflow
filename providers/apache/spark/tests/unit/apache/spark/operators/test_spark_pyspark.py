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
from airflow.providers.apache.spark.operators.spark_pyspark import PySparkOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2024, 2, 1, tzinfo=timezone.utc)


class TestSparkPySparkOperator:
    _config = {
        "conn_id": "spark_special_conn_id",
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

    def test_execute(self):
        def my_spark_fn(spark):
            pass

        operator = PySparkOperator(
            task_id="spark_pyspark_job", python_callable=my_spark_fn, dag=self.dag, **self._config
        )

        assert self._config["conn_id"] == operator.conn_id
