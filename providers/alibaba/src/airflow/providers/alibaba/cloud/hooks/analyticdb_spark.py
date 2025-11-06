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

import json
from collections.abc import Sequence
from enum import Enum
from typing import Any

from alibabacloud_adb20211201.client import Client
from alibabacloud_adb20211201.models import (
    GetSparkAppLogRequest,
    GetSparkAppStateRequest,
    GetSparkAppWebUiAddressRequest,
    KillSparkAppRequest,
    SubmitSparkAppRequest,
    SubmitSparkAppResponse,
)
from alibabacloud_tea_openapi.models import Config

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class AppState(Enum):
    """
    AnalyticDB Spark application states.

    See:
    https://www.alibabacloud.com/help/en/analyticdb-for-mysql/latest/api-doc-adb-2021-12-01-api-struct
    -sparkappinfo.

    """

    SUBMITTED = "SUBMITTED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    FAILING = "FAILING"
    FAILED = "FAILED"
    KILLING = "KILLING"
    KILLED = "KILLED"
    SUCCEEDING = "SUCCEEDING"
    COMPLETED = "COMPLETED"
    FATAL = "FATAL"
    UNKNOWN = "UNKNOWN"


class AnalyticDBSparkHook(BaseHook, LoggingMixin):
    """
    Hook for AnalyticDB MySQL Spark through the REST API.

    :param adb_spark_conn_id: The Airflow connection used for AnalyticDB MySQL Spark credentials.
    :param region: AnalyticDB MySQL region you want to submit spark application.
    """

    TERMINAL_STATES = {AppState.COMPLETED, AppState.FAILED, AppState.FATAL, AppState.KILLED}

    conn_name_attr = "adb_spark_conn_id"
    default_conn_name = "adb_spark_default"
    conn_type = "adb_spark"
    hook_name = "AnalyticDB Spark"

    def __init__(
        self, adb_spark_conn_id: str = "adb_spark_default", region: str | None = None, *args, **kwargs
    ) -> None:
        self.adb_spark_conn_id = adb_spark_conn_id
        self.adb_spark_conn = self.get_connection(adb_spark_conn_id)
        self.region = region or self.get_default_region()
        super().__init__(*args, **kwargs)

    def submit_spark_app(
        self, cluster_id: str, rg_name: str, *args: Any, **kwargs: Any
    ) -> SubmitSparkAppResponse:
        """
        Perform request to submit spark application.

        :param cluster_id: The cluster ID of AnalyticDB MySQL 3.0 Data Lakehouse.
        :param rg_name: The name of resource group in AnalyticDB MySQL 3.0 Data Lakehouse cluster.
        """
        self.log.info("Submitting application")
        request = SubmitSparkAppRequest(
            dbcluster_id=cluster_id,
            resource_group_name=rg_name,
            data=json.dumps(self.build_submit_app_data(*args, **kwargs)),
            app_type="BATCH",
        )
        try:
            return self.get_adb_spark_client().submit_spark_app(request)
        except Exception as e:
            self.log.error(e)
            raise AirflowException("Errors when submit spark application") from e

    def submit_spark_sql(
        self, cluster_id: str, rg_name: str, *args: Any, **kwargs: Any
    ) -> SubmitSparkAppResponse:
        """
        Perform request to submit spark sql.

        :param cluster_id: The cluster ID of AnalyticDB MySQL 3.0 Data Lakehouse.
        :param rg_name: The name of resource group in AnalyticDB MySQL 3.0 Data Lakehouse cluster.
        """
        self.log.info("Submitting Spark SQL")
        request = SubmitSparkAppRequest(
            dbcluster_id=cluster_id,
            resource_group_name=rg_name,
            data=self.build_submit_sql_data(*args, **kwargs),
            app_type="SQL",
        )
        try:
            return self.get_adb_spark_client().submit_spark_app(request)
        except Exception as e:
            self.log.error(e)
            raise AirflowException("Errors when submit spark sql") from e

    def get_spark_state(self, app_id: str) -> str:
        """
        Fetch the state of the specified spark application.

        :param app_id: identifier of the spark application
        """
        self.log.debug("Fetching state for spark application %s", app_id)
        try:
            return (
                self.get_adb_spark_client()
                .get_spark_app_state(GetSparkAppStateRequest(app_id=app_id))
                .body.data.state
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when fetching state for spark application: {app_id}") from e

    def get_spark_web_ui_address(self, app_id: str) -> str:
        """
        Fetch the web ui address of the specified spark application.

        :param app_id: identifier of the spark application
        """
        self.log.debug("Fetching web ui address for spark application %s", app_id)
        try:
            return (
                self.get_adb_spark_client()
                .get_spark_app_web_ui_address(GetSparkAppWebUiAddressRequest(app_id=app_id))
                .body.data.web_ui_address
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Errors when fetching web ui address for spark application: {app_id}"
            ) from e

    def get_spark_log(self, app_id: str) -> str:
        """
        Get the logs for a specified spark application.

        :param app_id: identifier of the spark application
        """
        self.log.debug("Fetching log for spark application %s", app_id)
        try:
            return (
                self.get_adb_spark_client()
                .get_spark_app_log(GetSparkAppLogRequest(app_id=app_id))
                .body.data.log_content
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when fetching log for spark application: {app_id}") from e

    def kill_spark_app(self, app_id: str) -> None:
        """
        Kill the specified spark application.

        :param app_id: identifier of the spark application
        """
        self.log.info("Killing spark application %s", app_id)
        try:
            self.get_adb_spark_client().kill_spark_app(KillSparkAppRequest(app_id=app_id))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when killing spark application: {app_id}") from e

    @staticmethod
    def build_submit_app_data(
        file: str | None = None,
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
    ) -> dict:
        """
        Build the submit application request data.

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
        """
        if file is None:
            raise ValueError("Parameter file is need when submit spark application.")

        data: dict[str, Any] = {"file": file}
        extra_conf: dict[str, str] = {}

        if class_name:
            data["className"] = class_name
        if args and AnalyticDBSparkHook._validate_list_of_stringables(args):
            data["args"] = [str(val) for val in args]
        if driver_resource_spec:
            extra_conf["spark.driver.resourceSpec"] = driver_resource_spec
        if executor_resource_spec:
            extra_conf["spark.executor.resourceSpec"] = executor_resource_spec
        if num_executors:
            extra_conf["spark.executor.instances"] = str(num_executors)
        data["conf"] = extra_conf.copy()
        if conf and AnalyticDBSparkHook._validate_extra_conf(conf):
            data["conf"].update(conf)
        if jars and AnalyticDBSparkHook._validate_list_of_stringables(jars):
            data["jars"] = jars
        if py_files and AnalyticDBSparkHook._validate_list_of_stringables(py_files):
            data["pyFiles"] = py_files
        if files and AnalyticDBSparkHook._validate_list_of_stringables(files):
            data["files"] = files
        if archives and AnalyticDBSparkHook._validate_list_of_stringables(archives):
            data["archives"] = archives
        if name:
            data["name"] = name

        return data

    @staticmethod
    def build_submit_sql_data(
        sql: str | None = None,
        conf: dict[Any, Any] | None = None,
        driver_resource_spec: str | None = None,
        executor_resource_spec: str | None = None,
        num_executors: int | str | None = None,
        name: str | None = None,
    ) -> str:
        """
        Build the submit spark sql request data.

        :param sql: The SQL query to execute. (templated)
        :param conf: Spark configuration properties.
        :param driver_resource_spec: The resource specifications of the Spark driver.
        :param executor_resource_spec: The resource specifications of each Spark executor.
        :param num_executors: number of executors to launch for this application.
        :param name: name of this application.
        """
        if sql is None:
            raise ValueError("Parameter sql is need when submit spark sql.")

        extra_conf: dict[str, str] = {}
        formatted_conf = ""

        if driver_resource_spec:
            extra_conf["spark.driver.resourceSpec"] = driver_resource_spec
        if executor_resource_spec:
            extra_conf["spark.executor.resourceSpec"] = executor_resource_spec
        if num_executors:
            extra_conf["spark.executor.instances"] = str(num_executors)
        if name:
            extra_conf["spark.app.name"] = name
        if conf and AnalyticDBSparkHook._validate_extra_conf(conf):
            extra_conf.update(conf)
        for key, value in extra_conf.items():
            formatted_conf += f"set {key} = {value};"

        return (formatted_conf + sql).strip()

    @staticmethod
    def _validate_list_of_stringables(vals: Sequence[str | int | float]) -> bool:
        """
        Check the values in the provided list can be converted to strings.

        :param vals: list to validate
        """
        if (
            vals is None
            or not isinstance(vals, (tuple, list))
            or not all(isinstance(val, (str, int, float)) for val in vals)
        ):
            raise ValueError("List of strings expected")
        return True

    @staticmethod
    def _validate_extra_conf(conf: dict[Any, Any]) -> bool:
        """
        Check configuration values are either strings or ints.

        :param conf: configuration variable
        """
        if conf:
            if not isinstance(conf, dict):
                raise ValueError("'conf' argument must be a dict")
            if not all(isinstance(v, (str, int)) and v != "" for v in conf.values()):
                raise ValueError("'conf' values must be either strings or ints")
        return True

    def get_adb_spark_client(self) -> Client:
        """Get valid AnalyticDB MySQL Spark client."""
        extra_config = self.adb_spark_conn.extra_dejson
        auth_type = extra_config.get("auth_type", None)
        if not auth_type:
            raise ValueError("No auth_type specified in extra_config.")

        if auth_type != "AK":
            raise ValueError(f"Unsupported auth_type: {auth_type}")
        adb_spark_access_key_id = extra_config.get("access_key_id", None)
        adb_spark_access_secret = extra_config.get("access_key_secret", None)
        if not adb_spark_access_key_id:
            raise ValueError(f"No access_key_id is specified for connection: {self.adb_spark_conn_id}")

        if not adb_spark_access_secret:
            raise ValueError(f"No access_key_secret is specified for connection: {self.adb_spark_conn_id}")

        return Client(
            Config(
                access_key_id=adb_spark_access_key_id,
                access_key_secret=adb_spark_access_secret,
                endpoint=f"adb.{self.region}.aliyuncs.com",
            )
        )

    def get_default_region(self) -> str:
        """Get default region from connection."""
        extra_config = self.adb_spark_conn.extra_dejson
        auth_type = extra_config.get("auth_type", None)
        if not auth_type:
            raise ValueError("No auth_type specified in extra_config. ")

        if auth_type != "AK":
            raise ValueError(f"Unsupported auth_type: {auth_type}")

        default_region = extra_config.get("region", None)
        if not default_region:
            raise ValueError(f"No region is specified for connection: {self.adb_spark_conn}")
        return default_region
