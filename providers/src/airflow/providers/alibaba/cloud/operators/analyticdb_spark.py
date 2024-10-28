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

import time
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from deprecated.classic import deprecated

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.alibaba.cloud.hooks.analyticdb_spark import (
    AnalyticDBSparkHook,
    AppState,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AnalyticDBSparkBaseOperator(BaseOperator):
    """Abstract base class that defines how users develop AnalyticDB Spark."""

    def __init__(
        self,
        *,
        adb_spark_conn_id: str = "adb_spark_default",
        region: str | None = None,
        polling_interval: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.app_id: str | None = None
        self.polling_interval = polling_interval

        self._adb_spark_conn_id = adb_spark_conn_id
        self._region = region

    @cached_property
    def hook(self) -> AnalyticDBSparkHook:
        """Get valid hook."""
        return AnalyticDBSparkHook(
            adb_spark_conn_id=self._adb_spark_conn_id, region=self._region
        )

    @deprecated(
        reason="use `hook` property instead.", category=AirflowProviderDeprecationWarning
    )
    def get_hook(self) -> AnalyticDBSparkHook:
        """Get valid hook."""
        return self.hook

    def execute(self, context: Context) -> Any: ...

    def monitor_application(self):
        self.log.info("Monitoring application with %s", self.app_id)

        if self.polling_interval > 0:
            self.poll_for_termination(self.app_id)

    def poll_for_termination(self, app_id: str) -> None:
        """
        Pool for spark application termination.

        :param app_id: id of the spark application to monitor
        """
        state = self.hook.get_spark_state(app_id)
        while AppState(state) not in AnalyticDBSparkHook.TERMINAL_STATES:
            self.log.debug("Application with id %s is in state: %s", app_id, state)
            time.sleep(self.polling_interval)
            state = self.hook.get_spark_state(app_id)
        self.log.info("Application with id %s terminated with state: %s", app_id, state)
        self.log.info(
            "Web ui address is %s for application with id %s",
            self.hook.get_spark_web_ui_address(app_id),
            app_id,
        )
        self.log.info(self.hook.get_spark_log(app_id))
        if AppState(state) != AppState.COMPLETED:
            raise AirflowException(f"Application {app_id} did not succeed")

    def on_kill(self) -> None:
        self.kill()

    def kill(self) -> None:
        """Delete the specified application."""
        if self.app_id is not None:
            self.hook.kill_spark_app(self.app_id)


class AnalyticDBSparkSQLOperator(AnalyticDBSparkBaseOperator):
    """
    Submits a Spark SQL application to the underlying cluster; wraps the AnalyticDB Spark REST API.

    :param sql: The SQL query to execute.
    :param conf: Spark configuration properties.
    :param driver_resource_spec: The resource specifications of the Spark driver.
    :param executor_resource_spec: The resource specifications of each Spark executor.
    :param num_executors: number of executors to launch for this application.
    :param name: name of this application.
    :param cluster_id: The cluster ID of AnalyticDB MySQL 3.0 Data Lakehouse.
    :param rg_name: The name of resource group in AnalyticDB MySQL 3.0 Data Lakehouse cluster.
    """

    template_fields: Sequence[str] = ("spark_params",)
    template_fields_renderers = {"spark_params": "json"}

    def __init__(
        self,
        *,
        sql: str,
        conf: dict[Any, Any] | None = None,
        driver_resource_spec: str | None = None,
        executor_resource_spec: str | None = None,
        num_executors: int | str | None = None,
        name: str | None = None,
        cluster_id: str,
        rg_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        spark_params = {
            "sql": sql,
            "conf": conf,
            "driver_resource_spec": driver_resource_spec,
            "executor_resource_spec": executor_resource_spec,
            "num_executors": num_executors,
            "name": name,
        }
        self.spark_params = spark_params
        self._cluster_id = cluster_id
        self._rg_name = rg_name

    def execute(self, context: Context) -> Any:
        submit_response = self.hook.submit_spark_sql(
            cluster_id=self._cluster_id, rg_name=self._rg_name, **self.spark_params
        )
        self.app_id = submit_response.body.data.app_id
        self.monitor_application()
        return self.app_id


class AnalyticDBSparkBatchOperator(AnalyticDBSparkBaseOperator):
    """
    Submits a Spark batch application to the underlying cluster; wraps the AnalyticDB Spark REST API.

    :param file: path of the file containing the application to execute.
    :param class_name: name of the application Java/Spark main class.
    :param args: application command line arguments.
    :param conf: Spark configuration properties.
    :param jars: jars to be used in this application.
    :param py_files: python files to be used in this application.
    :param files: files to be used in this application.
    :param driver_resource_spec: The resource specifications of the Spark driver.
    :param executor_resource_spec: The resource specifications of each Spark executor.
    :param num_executors: number of executors to launch for this application.
    :param archives: archives to be used in this application.
    :param name: name of this application.
    :param cluster_id: The cluster ID of AnalyticDB MySQL 3.0 Data Lakehouse.
    :param rg_name: The name of resource group in AnalyticDB MySQL 3.0 Data Lakehouse cluster.
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
        driver_resource_spec: str | None = None,
        executor_resource_spec: str | None = None,
        num_executors: int | str | None = None,
        archives: Sequence[str] | None = None,
        name: str | None = None,
        cluster_id: str,
        rg_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        spark_params = {
            "file": file,
            "class_name": class_name,
            "args": args,
            "conf": conf,
            "jars": jars,
            "py_files": py_files,
            "files": files,
            "driver_resource_spec": driver_resource_spec,
            "executor_resource_spec": executor_resource_spec,
            "num_executors": num_executors,
            "archives": archives,
            "name": name,
        }
        self.spark_params = spark_params
        self._cluster_id = cluster_id
        self._rg_name = rg_name

    def execute(self, context: Context) -> Any:
        submit_response = self.hook.submit_spark_app(
            cluster_id=self._cluster_id, rg_name=self._rg_name, **self.spark_params
        )
        self.app_id = submit_response.body.data.app_id
        self.monitor_application()
        return self.app_id
