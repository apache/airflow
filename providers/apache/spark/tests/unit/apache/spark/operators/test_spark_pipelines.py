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

from unittest.mock import MagicMock, patch

from airflow.providers.apache.spark.operators.spark_pipelines import SparkPipelinesOperator


class TestSparkPipelinesOperator:
    def test_init_with_run_command(self):
        operator = SparkPipelinesOperator(
            task_id="test_task", pipeline_spec="test_pipeline.yml", pipeline_command="run"
        )
        assert operator.pipeline_spec == "test_pipeline.yml"
        assert operator.pipeline_command == "run"

    def test_init_with_dry_run_command(self):
        operator = SparkPipelinesOperator(
            task_id="test_task", pipeline_spec="test_pipeline.yml", pipeline_command="dry-run"
        )
        assert operator.pipeline_command == "dry-run"

    def test_template_fields(self):
        expected_fields = (
            "pipeline_spec",
            "conf",
            "env_vars",
            "keytab",
            "principal",
        )
        operator = SparkPipelinesOperator(task_id="test_task")
        assert operator.template_fields == expected_fields

    def test_execute(self):
        mock_hook = MagicMock()

        with patch.object(SparkPipelinesOperator, "hook", mock_hook):
            operator = SparkPipelinesOperator(
                task_id="test_task", pipeline_spec="test_pipeline.yml", pipeline_command="run"
            )

            context = {}
            operator.execute(context)

            mock_hook.submit_pipeline.assert_called_once()

    def test_on_kill(self):
        mock_hook = MagicMock()

        with patch.object(SparkPipelinesOperator, "hook", mock_hook):
            operator = SparkPipelinesOperator(task_id="test_task", pipeline_spec="test_pipeline.yml")

            operator.on_kill()

            mock_hook.on_kill.assert_called_once()

    def test_get_hook(self):
        operator = SparkPipelinesOperator(
            task_id="test_task",
            pipeline_spec="test_pipeline.yml",
            pipeline_command="run",
            conf={"spark.sql.adaptive.enabled": "true"},
            num_executors=2,
            executor_cores=4,
            executor_memory="2G",
            driver_memory="1G",
            verbose=True,
            env_vars={"SPARK_HOME": "/opt/spark"},
            deploy_mode="client",
            yarn_queue="default",
            keytab="/path/to/keytab",
            principal="user@REALM.COM",
        )

        hook = operator.hook

        assert hook.pipeline_spec == "test_pipeline.yml"
        assert hook.pipeline_command == "run"
        assert hook._conf == {"spark.sql.adaptive.enabled": "true"}
        assert hook._num_executors == 2
        assert hook._executor_cores == 4
        assert hook._executor_memory == "2G"
        assert hook._driver_memory == "1G"
        assert hook._verbose is True
        assert hook._env_vars == {"SPARK_HOME": "/opt/spark"}
        assert hook._deploy_mode == "client"
        assert hook._yarn_queue == "default"
        assert hook._keytab == "/path/to/keytab"
        assert hook._principal == "user@REALM.COM"

    @patch(
        "airflow.providers.apache.spark.operators.spark_pipelines.inject_parent_job_information_into_spark_properties"
    )
    def test_execute_with_openlineage_parent_job_info(self, mock_inject_parent):
        mock_hook = MagicMock()

        with patch.object(SparkPipelinesOperator, "hook", mock_hook):
            original_conf = {"spark.sql.adaptive.enabled": "true"}
            modified_conf = {**original_conf, "spark.openlineage.parentJobName": "test_job"}
            mock_inject_parent.return_value = modified_conf

            operator = SparkPipelinesOperator(
                task_id="test_task",
                pipeline_spec="test_pipeline.yml",
                conf=original_conf,
                openlineage_inject_parent_job_info=True,
            )

            context = {"task_instance": MagicMock()}
            operator.execute(context)

            mock_inject_parent.assert_called_once_with(original_conf, context)
            assert operator.conf == modified_conf
            mock_hook.submit_pipeline.assert_called_once()

    @patch(
        "airflow.providers.apache.spark.operators.spark_pipelines.inject_transport_information_into_spark_properties"
    )
    def test_execute_with_openlineage_transport_info(self, mock_inject_transport):
        mock_hook = MagicMock()

        with patch.object(SparkPipelinesOperator, "hook", mock_hook):
            original_conf = {"spark.sql.adaptive.enabled": "true"}
            modified_conf = {**original_conf, "spark.openlineage.transport.type": "http"}
            mock_inject_transport.return_value = modified_conf

            operator = SparkPipelinesOperator(
                task_id="test_task",
                pipeline_spec="test_pipeline.yml",
                conf=original_conf,
                openlineage_inject_transport_info=True,
            )

            context = {"task_instance": MagicMock()}
            operator.execute(context)

            mock_inject_transport.assert_called_once_with(original_conf, context)
            assert operator.conf == modified_conf
            mock_hook.submit_pipeline.assert_called_once()
