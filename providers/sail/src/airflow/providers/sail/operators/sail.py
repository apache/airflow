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

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.compat.standard.operators import PythonOperator
from airflow.providers.sail.hooks.sail import SailHook

SPARK_CONTEXT_KEYS = ["spark", "sc"]

DEFAULT_LOCAL_PORT = 50051


class PySailOperator(PythonOperator):
    """
    Execute PySpark code using a Sail engine.

    Supports two modes of operation:

    - **Remote**: when ``conn_id`` is provided and the connection has a host,
      connects to an existing Sail server via the Spark Connect protocol.
    - **Local embedded**: when no ``conn_id`` is provided (or the connection
      has no host), starts a local Sail server, executes the callable, and
      stops the server afterwards.

    The ``python_callable`` receives a ``spark`` argument (a ``SparkSession``
    connected to the Sail server).

    :param python_callable: A Python callable to execute. It will receive a
        ``spark`` keyword argument with an active ``SparkSession``.
    :param conn_id: Airflow connection ID for a remote Sail server.
        When ``None``, a local embedded server is started.
    :param config_kwargs: Additional Spark configuration key-value pairs.
    :param local_server_port: Port for the local embedded server.
        Defaults to ``50051``.
    """

    template_fields: Sequence[str] = ("conn_id", "config_kwargs", *PythonOperator.template_fields)

    def __init__(
        self,
        python_callable: Callable,
        conn_id: str | None = None,
        config_kwargs: dict | None = None,
        local_server_port: int = DEFAULT_LOCAL_PORT,
        **kwargs,
    ):
        self.conn_id = conn_id
        self.config_kwargs = config_kwargs or {}
        self.local_server_port = local_server_port

        signature = inspect.signature(python_callable)
        parameters = [
            param.replace(default=None) if param.name in SPARK_CONTEXT_KEYS else param
            for param in signature.parameters.values()
        ]
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

        server = None
        url: str | None = None

        if self.conn_id:
            conn = BaseHook.get_connection(self.conn_id)
            if conn.host:
                hook = SailHook(self.conn_id)
                url = hook.get_connection_url()
            for key, value in conn.extra_dejson.items():
                if key != SailHook.PARAM_USE_SSL:
                    conf.set(key, value)

        if url is None:
            # No remote connection — start a local embedded Sail server.
            hook = SailHook.__new__(SailHook)
            hook.__init__()  # type: ignore[misc]
            server = hook.start_local_server(port=self.local_server_port, background=True)
            url = f"sc://localhost:{self.local_server_port}"

        conf.set("spark.remote", url)

        for key, value in self.config_kwargs.items():
            conf.set(key, value)

        spark_session = SparkSession.builder.config(conf=conf).getOrCreate()

        try:
            self.op_kwargs = {**self.op_kwargs, "spark": spark_session}
            return super().execute_callable()
        finally:
            spark_session.stop()
            if server is not None:
                self.log.info("Stopping local Sail server")
                server.stop()
