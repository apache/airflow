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
from typing import TYPE_CHECKING, Any, Callable, Sequence

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.hooks.spark_connect import SparkConnectHook
from airflow.providers.standard.operators.python import PythonOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

SPARK_CONTEXT_KEYS = ["spark", "sc"]


class _PySparkDecoratedOperator(DecoratedOperator, PythonOperator):
    custom_operator_name = "@task.pyspark"

    template_fields: Sequence[str] = ("op_args", "op_kwargs")

    def __init__(
        self,
        python_callable: Callable,
        op_args: Sequence | None = None,
        op_kwargs: dict | None = None,
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

        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )

    def execute(self, context: Context):
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

        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        if not self.op_kwargs:
            self.op_kwargs = {}

        op_kwargs: dict[str, Any] = dict(self.op_kwargs)
        op_kwargs["spark"] = spark

        # spark context is not available when using spark connect
        op_kwargs["sc"] = spark.sparkContext if not conf.get("spark.remote") else None

        self.op_kwargs = op_kwargs
        return super().execute(context)


def pyspark_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_PySparkDecoratedOperator,
        **kwargs,
    )
