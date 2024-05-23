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

from airflow.providers.alibaba.cloud.operators.emr import EmrServerlessSparkStartJobRunOperator

EMR_OPERATOR_STRING = "airflow.providers.alibaba.cloud.operators.emr.{}"

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


class TestEmrServerlessSparkStartJobRunOperator:
    def setup_method(self):
        self.operator = EmrServerlessSparkStartJobRunOperator(
            task_id=MOCK_TASK_ID,
            emr_serverless_spark_conn_id=MOCK_EMR_CONN_ID,
            region=MOCK_REGION,
            polling_interval=5,
            workspace_id=MOCK_WORKSPACE_ID,
            resource_queue_id=MOCK_QUEUE_ID,
            code_type=MOCK_CODE_TYPE,
            name=MOCK_JOB_NAME,
            engine_release_version=MOCK_RELEASE_VERSION,
            entry_point=MOCK_ENTRY_POINT,
            entry_point_args=MOCK_ENTRY_POINT_ARGS,
            spark_submit_parameters=MOCK_SPARK_SUBMIT_PARAMETERS,
            is_prod=True,
        )

    @mock.patch(EMR_OPERATOR_STRING.format("EmrServerlessSparkHook"))
    def test_get_hook(self, mock_hook):
        """Test get_hook function works as expected."""
        self.operator.hook
        mock_hook.assert_called_once_with(
            emr_serverless_spark_conn_id=MOCK_EMR_CONN_ID, region=MOCK_REGION, workspace_id=MOCK_WORKSPACE_ID
        )

    @mock.patch(EMR_OPERATOR_STRING.format("EmrServerlessSparkStartJobRunOperator.poll_job_run_state"))
    @mock.patch(EMR_OPERATOR_STRING.format("EmrServerlessSparkHook"))
    def test_execute(self, mock_hook, mock_poll_job_run_state):
        self.operator.execute(None)
        mock_hook.assert_called_once_with(
            emr_serverless_spark_conn_id=MOCK_EMR_CONN_ID, region=MOCK_REGION, workspace_id=MOCK_WORKSPACE_ID
        )
        mock_hook.return_value.start_job_run.assert_called_once_with(
            resource_queue_id=MOCK_QUEUE_ID,
            code_type=MOCK_CODE_TYPE,
            name=MOCK_JOB_NAME,
            engine_release_version=MOCK_RELEASE_VERSION,
            entry_point=MOCK_ENTRY_POINT,
            entry_point_args=MOCK_ENTRY_POINT_ARGS,
            spark_submit_parameters=MOCK_SPARK_SUBMIT_PARAMETERS,
            is_prod=True,
        )
        mock_poll_job_run_state.assert_called_once_with()

    @mock.patch(EMR_OPERATOR_STRING.format("EmrServerlessSparkStartJobRunOperator.poll_for_termination"))
    def test_poll_job_run_state(self, mock_poll_for_termination):
        self.operator.job_run_id = MOCK_JOB_RUN_ID
        self.operator.poll_job_run_state()
        mock_poll_for_termination.assert_called_once_with(MOCK_JOB_RUN_ID)

    @mock.patch(EMR_OPERATOR_STRING.format("EmrServerlessSparkStartJobRunOperator.kill"))
    def test_on_kill(self, mock_kill):
        self.operator.on_kill()
        mock_kill.assert_called_once_with()

    @mock.patch(EMR_OPERATOR_STRING.format("EmrServerlessSparkHook"))
    def test_kill(self, mock_hook):
        self.operator.job_run_id = MOCK_JOB_RUN_ID
        self.operator.kill()
        mock_hook.return_value.cancel_job_run.assert_called_once_with(MOCK_JOB_RUN_ID)
