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
from enum import Enum
from typing import Any, Sequence, List, Dict

from alibabacloud_emr_serverless_spark20230808.client import Client

from alibabacloud_emr_serverless_spark20230808.models import (
    StartJobRunRequest,
    StartJobRunResponse,
    GetJobRunRequest,
    GetJobRunResponse,
    CancelJobRunRequest,
    CancelJobRunResponse,
    Tag,
    JobDriver,
    JobDriverSparkSubmit,
)

from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util import models as util_models

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class AppState(Enum):
    """
    EMR Serverless Spark Job Run States

    """

    SUBMITTED = "Submitted"
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCESS = "Success"
    FAILED = "Failed"
    CANCELLING = "Cancelling"
    CANCELLED = "Cancelled"
    CANCEL_FAILED = "CancelFailed"


class EmrServerlessSparkHook(BaseHook, LoggingMixin):
    TERMINAL_STATES = {AppState.SUCCESS, AppState.FAILED, AppState.CANCELLED, AppState.CANCEL_FAILED}

    conn_name_attr = "alibabacloud_conn_id"
    default_conn_name = "emr_serverless_spark_default"
    conn_type = "emr_serverless_spark"
    hook_name = "EMR Serverless Spark"

    def __init__(
        self,
        emr_serverless_spark_conn_id: str = "emr_serverless_spark_default",
        region: str | None = None,
        workspace_id: str | None = None,
        *args,
        **kwargs
    ) -> None:
        self.emr_serverless_spark_conn_id = emr_serverless_spark_conn_id
        self.emr_serverless_spark_conn = self.get_connection(emr_serverless_spark_conn_id)
        self.region = region or self.get_default_region()
        self.workspace_id = workspace_id
        self.client = self.get_client()
        super().__init__(*args, **kwargs)

    def start_job_run(
        self,
        resource_queue_id: str,
        code_type: str,
        name: str,
        engine_release_version: str | None,
        entry_point: str,
        entry_point_args: str,
        spark_submit_parameters: str,
        is_prod: bool
    ) -> StartJobRunResponse:

        env = "production" if is_prod else "dev"
        self.log.info("Submitting application")
        tags: List[Tag] = [Tag("environment", env), Tag("workflow", "true")]
        engine_release_version = "esr-2.1-native (Spark 3.3.1, Scala 2.12, Native Runtime)" if (
                engine_release_version is None) else engine_release_version
        job_driver_spark_submit = JobDriverSparkSubmit(
            entry_point,
            entry_point_args.split(';'),
            spark_submit_parameters
        )

        job_driver = JobDriver(job_driver_spark_submit)

        start_job_run_request = StartJobRunRequest(
            region_id=self.region,
            resource_queue_id=resource_queue_id,
            code_type=code_type,
            name=name,
            release_version=engine_release_version,
            tags=tags,
            job_driver=job_driver
        )

        runtime = util_models.RuntimeOptions()
        headers = {}

        try:
            return self.client.start_job_run_with_options(self.workspace_id, start_job_run_request, headers,
                                                          runtime)
        except Exception as e:
            self.log.error(e)
            raise AirflowException("Errors when starting job run") from e

    def get_job_run_state(self, job_run_id: str) -> str:
        self.log.debug("Polling state for job run - %s", job_run_id)
        try:
            return (
                self.client
                .get_job_run(self.workspace_id, job_run_id, GetJobRunRequest(region_id=self.region))
                .body.job_run.state
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when polling state for job run - {job_run_id}") from e

    def cancel_job_run(self, job_run_id: str) -> None:
        self.log.info("Canceling job run - %s", job_run_id)
        try:
            self.client.cancel_job_run(self.workspace_id, job_run_id, CancelJobRunRequest(region_id=self.region))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when canceling job run: {job_run_id}") from e

    def get_client(self) -> Client:
        extra_config = self.emr_serverless_spark_conn.extra_dejson
        auth_type = extra_config.get("auth_type", None)
        if not auth_type:
            raise ValueError("No auth_type specified in extra_config.")

        if auth_type != "AK":
            raise ValueError(f"Unsupported auth_type: {auth_type}")
        access_key_id = extra_config.get("access_key_id", None)
        access_key_secret = extra_config.get("access_key_secret", None)
        if not access_key_id:
            raise ValueError(
                f"No access_key_id is specified for connection: {self.emr_serverless_spark_conn_id}")

        if not access_key_secret:
            raise ValueError(
                f"No access_key_secret is specified for connection: {self.emr_serverless_spark_conn_id}")

        return Client(
            Config(
                access_key_id=access_key_id,
                access_key_secret=access_key_secret,
                endpoint=f'emr-serverless-spark.{self.region}.aliyuncs.com',
            )
        )

    def get_default_region(self) -> str:
        """Get default region from connection."""
        extra_config = self.emr_serverless_spark_conn.extra_dejson
        auth_type = extra_config.get("auth_type", None)
        if not auth_type:
            raise ValueError("No auth_type specified in extra_config. ")

        if auth_type != "AK":
            raise ValueError(f"Unsupported auth_type: {auth_type}")

        default_region = extra_config.get("region", None)
        if not default_region:
            raise ValueError(f"No region is specified for connection: {self.emr_serverless_spark_conn}")
        return default_region
