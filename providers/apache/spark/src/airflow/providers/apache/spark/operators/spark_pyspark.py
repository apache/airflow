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

import inspect
from collections.abc import Callable, Sequence

from airflow.providers.apache.spark.hooks.spark_connect import SparkConnectHook
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.compat.standard.operators import PythonOperator

SPARK_CONTEXT_KEYS = ["spark", "sc"]


class PySparkOperator(PythonOperator):
    """Submit the run of a pyspark job to an external spark-connect service or directly run the pyspark job in a standalone mode."""

    template_fields: Sequence[str] = ("conn_id", "config_kwargs", *PythonOperator.template_fields)

    def __init__(
        self,
        python_callable: Callable,
        conn_id: str | None = None,
        config_kwargs: dict | None = None,
        **kwargs,
    ):
        self.conn_id = conn_id
        self.config_kwargs = config_kwargs or {}

        signature = inspect.signature(python_callable)
        parameters = [
            param.replace(default=None) if param.name in SPARK_CONTEXT_KEYS else param
            for param in signature.parameters.values()
        ]
        # mypy does not understand __signature__ attribute
        # see https://github.com/python/mypy/issues/12472
        python_callable.__signature__ = signature.replace(parameters=parameters)  # type: ignore[attr-defined]

        super().__init__(
            python_callable=python_callable,
            **kwargs,
        )

    def execute_callable(self):
        from pyspark import SparkConf
        from pyspark.sql import SparkSession

        conf = SparkConf()
        conf.set("spark.app.name", f"{self.dag_id}-{self.task_id}")

        url = "local[*]"
        if self.conn_id:
            # we handle both spark connect and spark standalone
            conn = BaseHook.get_connection(self.conn_id)
            if conn.conn_type == SparkConnectHook.conn_type:
                url = SparkConnectHook(self.conn_id).get_connection_url()
            elif conn.port:
                url = f"{conn.host}:{conn.port}"
            elif conn.host:
                url = conn.host

            for key, value in conn.extra_dejson.items():
                conf.set(key, value)

        # you cannot have both remote and master
        if url.startswith("sc://"):
            conf.set("spark.remote", url)

        # task can override connection config
        for key, value in self.config_kwargs.items():
            conf.set(key, value)

        if not conf.get("spark.remote") and not conf.get("spark.master"):
            conf.set("spark.master", url)

        spark_session = SparkSession.builder.config(conf=conf).getOrCreate()

        try:
            self.op_kwargs = {**self.op_kwargs, "spark": spark_session}
            return super().execute_callable()
        finally:
            spark_session.stop()
