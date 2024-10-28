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
"""This module contains the Apache Livy operator."""

from __future__ import annotations

import time
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from deprecated.classic import deprecated

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.apache.livy.hooks.livy import BatchState, LivyHook
from airflow.providers.apache.livy.triggers.livy import LivyTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LivyOperator(BaseOperator):
    """
    Wraps the Apache Livy batch REST API, allowing to submit a Spark application to the underlying cluster.

    :param file: path of the file containing the application to execute (required). (templated)
    :param class_name: name of the application Java/Spark main class. (templated)
    :param args: application command line arguments. (templated)
    :param jars: jars to be used in this sessions. (templated)
    :param py_files: python files to be used in this session. (templated)
    :param files: files to be used in this session. (templated)
    :param driver_memory: amount of memory to use for the driver process. (templated)
    :param driver_cores: number of cores to use for the driver process. (templated)
    :param executor_memory: amount of memory to use per executor process. (templated)
    :param executor_cores: number of cores to use for each executor. (templated)
    :param num_executors: number of executors to launch for this session. (templated)
    :param archives: archives to be used in this session. (templated)
    :param queue: name of the YARN queue to which the application is submitted. (templated)
    :param name: name of this session. (templated)
    :param conf: Spark configuration properties. (templated)
    :param proxy_user: user to impersonate when running the job. (templated)
    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :param livy_conn_auth_type: The auth type for the Livy Connection.
    :param polling_interval: time in seconds between polling for job completion. Don't poll for values <= 0
    :param extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    :param extra_headers: A dictionary of headers passed to the HTTP request to livy.
    :param retry_args: Arguments which define the retry behaviour.
        See Tenacity documentation at https://github.com/jd/tenacity
    :param deferrable: Run operator in the deferrable mode
    """

    template_fields: Sequence[str] = ("spark_params",)
    template_fields_renderers = {"spark_params": "json"}

    def __init__(
        self,
        *,
        file: str,
        class_name: str | None = None,
        args: Sequence[str | int | float] | None = None,
        conf: dict[Any, Any] | None = None,
        jars: Sequence[str] | None = None,
        py_files: Sequence[str] | None = None,
        files: Sequence[str] | None = None,
        driver_memory: str | None = None,
        driver_cores: int | str | None = None,
        executor_memory: str | None = None,
        executor_cores: int | str | None = None,
        num_executors: int | str | None = None,
        archives: Sequence[str] | None = None,
        queue: str | None = None,
        name: str | None = None,
        proxy_user: str | None = None,
        livy_conn_id: str = "livy_default",
        livy_conn_auth_type: Any | None = None,
        polling_interval: int = 0,
        extra_options: dict[str, Any] | None = None,
        extra_headers: dict[str, Any] | None = None,
        retry_args: dict[str, Any] | None = None,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        spark_params = {
            # Prepare spark parameters, it will be templated later.
            "file": file,
            "class_name": class_name,
            "args": args,
            "jars": jars,
            "py_files": py_files,
            "files": files,
            "driver_memory": driver_memory,
            "driver_cores": driver_cores,
            "executor_memory": executor_memory,
            "executor_cores": executor_cores,
            "num_executors": num_executors,
            "archives": archives,
            "queue": queue,
            "name": name,
            "conf": conf,
            "proxy_user": proxy_user,
        }
        self.spark_params = spark_params
        self._livy_conn_id = livy_conn_id
        self._livy_conn_auth_type = livy_conn_auth_type
        self._polling_interval = polling_interval
        self._extra_options = extra_options or {}
        self._extra_headers = extra_headers or {}

        self._batch_id: int | str | None = None
        self.retry_args = retry_args
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> LivyHook:
        """
        Get valid hook.

        :return: LivyHook
        """
        return LivyHook(
            livy_conn_id=self._livy_conn_id,
            extra_headers=self._extra_headers,
            extra_options=self._extra_options,
            auth_type=self._livy_conn_auth_type,
        )

    @deprecated(
        reason="use `hook` property instead.", category=AirflowProviderDeprecationWarning
    )
    def get_hook(self) -> LivyHook:
        """Get valid hook."""
        return self.hook

    def execute(self, context: Context) -> Any:
        self._batch_id = self.hook.post_batch(**self.spark_params)
        self.log.info("Generated batch-id is %s", self._batch_id)

        # Wait for the job to complete
        if not self.deferrable:
            if self._polling_interval > 0:
                self.poll_for_termination(self._batch_id)
            context["ti"].xcom_push(
                key="app_id", value=self.hook.get_batch(self._batch_id)["appId"]
            )
            return self._batch_id

        state = self.hook.get_batch_state(self._batch_id, retry_args=self.retry_args)
        self.log.debug("Batch with id %s is in state: %s", self._batch_id, state.value)
        if state not in self.hook.TERMINAL_STATES:
            self.defer(
                timeout=self.execution_timeout,
                trigger=LivyTrigger(
                    batch_id=self._batch_id,
                    spark_params=self.spark_params,
                    livy_conn_id=self._livy_conn_id,
                    polling_interval=self._polling_interval,
                    extra_options=self._extra_options,
                    extra_headers=self._extra_headers,
                    execution_timeout=self.execution_timeout,
                ),
                method_name="execute_complete",
            )
        else:
            self.log.info(
                "Batch with id %s terminated with state: %s", self._batch_id, state.value
            )
            self.hook.dump_batch_logs(self._batch_id)
            if state != BatchState.SUCCESS:
                raise AirflowException(f"Batch {self._batch_id} did not succeed")

            context["ti"].xcom_push(
                key="app_id", value=self.hook.get_batch(self._batch_id)["appId"]
            )
            return self._batch_id

    def poll_for_termination(self, batch_id: int | str) -> None:
        """
        Pool Livy for batch termination.

        :param batch_id: id of the batch session to monitor.
        """
        state = self.hook.get_batch_state(batch_id, retry_args=self.retry_args)
        while state not in self.hook.TERMINAL_STATES:
            self.log.debug("Batch with id %s is in state: %s", batch_id, state.value)
            time.sleep(self._polling_interval)
            state = self.hook.get_batch_state(batch_id, retry_args=self.retry_args)
        self.log.info("Batch with id %s terminated with state: %s", batch_id, state.value)
        self.hook.dump_batch_logs(batch_id)
        if state != BatchState.SUCCESS:
            raise AirflowException(f"Batch {batch_id} did not succeed")

    def on_kill(self) -> None:
        self.kill()

    def kill(self) -> None:
        """Delete the current batch session."""
        if self._batch_id is not None:
            self.hook.delete_batch(self._batch_id)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        # dump the logs from livy to worker through triggerer.
        if event.get("log_lines", None) is not None:
            for log_line in event["log_lines"]:
                self.log.info(log_line)

        if event["status"] == "timeout":
            self.hook.delete_batch(event["batch_id"])

        if event["status"] in ["error", "timeout"]:
            raise AirflowException(event["response"])

        self.log.info(
            "%s completed with response %s",
            self.task_id,
            event["response"],
        )
        context["ti"].xcom_push(
            key="app_id", value=self.hook.get_batch(event["batch_id"])["appId"]
        )
        return event["batch_id"]
