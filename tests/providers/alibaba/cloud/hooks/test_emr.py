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

from unittest import mock

from airflow.providers.alibaba.cloud.hooks.emr import EmrServerlessSparkHook
from tests.providers.alibaba.cloud.utils.emr_mock import mock_emr_hook_default_project_id

EMR_HOOK_STRING = "airflow.providers.alibaba.cloud.hooks.emr.{}"

MOCK_TASK_ID = "serverless-spark-task"
MOCK_REGION = "mock_region"
MOCK_EMR_CONN_ID = "mock_emr_conn_id"
MOCK_WORKSPACE_ID = "w-975bcfda96258dd7"
MOCK_QUEUE_ID = "root_queue"
MOCK_CODE_TYPE = "JAR"
MOCK_JOB_NAME = "airflow-spark-job"
MOCK_RELEASE_VERSION = "esr-2.1-native (Spark 3.3.1, Scala 2.12, Native Runtime)"
MOCK_ENTRY_POINT = "oss://datadev-oss-hdfs-test/spark-resource/examples/jars/spark-examples_2.12-3.3.1.jar"
MOCK_ENTRY_POINT_ARGS = ["1000"]
MOCK_SPARK_SUBMIT_PARAMETERS = (
    "--class org.apache.spark.examples.SparkPi --conf spark.executor.cores=4 "
    "--conf spark.executor.memory=20g --conf spark.driver.cores=4 --conf "
    "spark.driver.memory=8g --conf spark.executor.instances=1 "
)
MOCK_JOB_RUN_ID = "jr-c6ec2bc17fb12c4d"
MOCK_ACCESS_KEY_ID = "mock_access_key_id"
MOCK_ACCESS_KEY_SECRET = "mock_access_key_secret"


class TestEmrServerlessSparkHook:
    def setup_method(self):
        with mock.patch(
            EMR_HOOK_STRING.format("EmrServerlessSparkHook.__init__"),
            new=mock_emr_hook_default_project_id,
        ):
            self.hook = EmrServerlessSparkHook(emr_serverless_spark_conn_id=MOCK_EMR_CONN_ID)

        self.hook.workspace_id = MOCK_WORKSPACE_ID

    @mock.patch(EMR_HOOK_STRING.format("util_models.RuntimeOptions"))
    @mock.patch(EMR_HOOK_STRING.format("StartJobRunRequest"))
    @mock.patch(EMR_HOOK_STRING.format("EmrServerlessSparkHook.get_client"))
    def test_start_job_run(self, mock_get_client, mock_start_job_run_request, mock_run_time_options):
        # Given
        mock_client = mock_get_client.return_value
        self.hook.client = mock_client

        # When
        self.hook.start_job_run(
            resource_queue_id=MOCK_QUEUE_ID,
            code_type=MOCK_CODE_TYPE,
            name=MOCK_JOB_NAME,
            engine_release_version=MOCK_RELEASE_VERSION,
            entry_point=MOCK_ENTRY_POINT,
            entry_point_args=MOCK_ENTRY_POINT_ARGS,
            spark_submit_parameters=MOCK_SPARK_SUBMIT_PARAMETERS,
            is_prod=True,
        )

        # Then
        mock_client.start_job_run_with_options.assert_called_once_with(
            MOCK_WORKSPACE_ID, mock_start_job_run_request.return_value, {}, mock_run_time_options.return_value
        )

    @mock.patch(EMR_HOOK_STRING.format("GetJobRunRequest"))
    @mock.patch(EMR_HOOK_STRING.format("EmrServerlessSparkHook.get_client"))
    def test_get_job_run_state(self, mock_get_client, mock_get_job_request):
        # Given
        mock_client = mock_get_client.return_value
        self.hook.client = mock_client

        # When
        self.hook.get_job_run_state(MOCK_JOB_RUN_ID)

        # Then
        mock_client.get_job_run.assert_called_once_with(
            MOCK_WORKSPACE_ID, MOCK_JOB_RUN_ID, mock_get_job_request.return_value
        )
        mock_get_job_request.assert_called_once_with(region_id=MOCK_REGION)

    @mock.patch(EMR_HOOK_STRING.format("GetJobRunRequest"))
    @mock.patch(EMR_HOOK_STRING.format("EmrServerlessSparkHook.get_client"))
    def test_get_job_run(self, mock_get_client, mock_get_job_request):
        # Given
        mock_client = mock_get_client.return_value
        self.hook.client = mock_client

        # When
        self.hook.get_job_run(MOCK_JOB_RUN_ID)

        # Then
        mock_client.get_job_run.assert_called_once_with(
            MOCK_WORKSPACE_ID, MOCK_JOB_RUN_ID, mock_get_job_request.return_value
        )
        mock_get_job_request.assert_called_once_with(region_id=MOCK_REGION)

    @mock.patch(EMR_HOOK_STRING.format("CancelJobRunRequest"))
    @mock.patch(EMR_HOOK_STRING.format("EmrServerlessSparkHook.get_client"))
    def test_cancel_job_run(self, mock_get_client, mock_cancel_job_request):
        # Given
        mock_client = mock_get_client.return_value
        self.hook.client = mock_client

        # When
        self.hook.cancel_job_run(MOCK_JOB_RUN_ID)

        # Then
        mock_client.cancel_job_run.assert_called_once_with(
            MOCK_WORKSPACE_ID, MOCK_JOB_RUN_ID, mock_cancel_job_request.return_value
        )
        mock_cancel_job_request.assert_called_once_with(region_id=MOCK_REGION)

    @mock.patch(EMR_HOOK_STRING.format("Config"))
    @mock.patch(EMR_HOOK_STRING.format("Client"))
    def test_get_client(self, mock_client_init, mock_config_init):
        mock_config = mock_config_init.return_value

        # When
        self.hook.get_client()

        # Then
        mock_client_init.assert_called_once_with(mock_config)
        mock_config_init.assert_called_once_with(
            access_key_id=MOCK_ACCESS_KEY_ID,
            access_key_secret=MOCK_ACCESS_KEY_SECRET,
            endpoint=f"emr-serverless-spark.{MOCK_REGION}.aliyuncs.com",
        )
