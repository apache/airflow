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

import logging
from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.apache.livy.hooks.livy import BatchState
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
BATCH_ID = 100
APP_ID = "application_1433865536131_34483"
GET_BATCH = {"appId": APP_ID}
LOG_RESPONSE = {"total": 3, "log": ["first_line", "second_line", "third_line"]}


class TestLivyOperator:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)
        create_connection_without_db(
            Connection(
                conn_id="livyunittest", conn_type="livy", host="localhost:8998", port="8998", schema="http"
            )
        )
        self.mock_context = dict(ti=MagicMock())

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state")
    def test_poll_for_termination(self, mock_livy, mock_dump_logs):
        state_list = 2 * [BatchState.RUNNING] + [BatchState.SUCCESS]

        def side_effect(_, retry_args):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperator(file="sparkapp", polling_interval=1, dag=self.dag, task_id="livy_example")
        task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_with(BATCH_ID)
        assert mock_livy.call_count == 3

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state")
    def test_poll_for_termination_fail(self, mock_livy, mock_dump_logs):
        state_list = 2 * [BatchState.RUNNING] + [BatchState.ERROR]

        def side_effect(_, retry_args):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperator(file="sparkapp", polling_interval=1, dag=self.dag, task_id="livy_example")

        with pytest.raises(AirflowException):
            task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_with(BATCH_ID)
        assert mock_livy.call_count == 3

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state",
        return_value=BatchState.SUCCESS,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    def test_execution(self, mock_get_batch, mock_post, mock_get, mock_dump_logs):
        task = LivyOperator(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=self.dag,
            task_id="livy_example",
        )
        task.execute(context=self.mock_context)

        call_args = {k: v for k, v in mock_post.call_args.kwargs.items() if v}
        assert call_args == {"file": "sparkapp"}
        mock_get.assert_called_once_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_once_with(BATCH_ID)
        mock_get_batch.assert_called_once_with(BATCH_ID)
        self.mock_context["ti"].xcom_push.assert_called_once_with(key="app_id", value=APP_ID)

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch")
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    def test_execution_with_extra_options(self, mock_get_batch, mock_post):
        extra_options = {"check_response": True}
        task = LivyOperator(
            file="sparkapp", dag=self.dag, task_id="livy_example", extra_options=extra_options
        )

        task.execute(context=self.mock_context)

        assert task.hook.extra_options == extra_options

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.delete_batch")
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    def test_deletion(self, mock_get_batch, mock_post, mock_delete):
        task = LivyOperator(
            livy_conn_id="livyunittest", file="sparkapp", dag=self.dag, task_id="livy_example"
        )
        task.execute(context=self.mock_context)
        task.kill()

        mock_delete.assert_called_once_with(BATCH_ID)

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state")
    def test_poll_for_termination_deferrable(self, mock_livy, mock_dump_logs):
        state_list = 2 * [BatchState.RUNNING] + [BatchState.SUCCESS]

        def side_effect(_, retry_args):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperator(
            file="sparkapp", polling_interval=1, dag=self.dag, task_id="livy_example", deferrable=True
        )
        task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_with(BATCH_ID)
        assert mock_livy.call_count == 3

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state")
    def test_poll_for_termination_fail_deferrable(self, mock_livy, mock_dump_logs):
        state_list = 2 * [BatchState.RUNNING] + [BatchState.ERROR]

        def side_effect(_, retry_args):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperator(
            file="sparkapp", polling_interval=1, dag=self.dag, task_id="livy_example", deferrable=True
        )

        with pytest.raises(AirflowException):
            task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_with(BATCH_ID)
        assert mock_livy.call_count == 3

    @patch("airflow.providers.apache.livy.operators.livy.LivyOperator.defer")
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state",
        return_value=BatchState.SUCCESS,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    def test_execution_deferrable(self, mock_get_batch, mock_post, mock_get, mock_dump_logs, mock_defer):
        task = LivyOperator(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=self.dag,
            task_id="livy_example",
            deferrable=True,
        )
        task.execute(context=self.mock_context)
        assert not mock_defer.called
        call_args = {k: v for k, v in mock_post.call_args[1].items() if v}
        assert call_args == {"file": "sparkapp"}
        mock_get.assert_called_once_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_once_with(BATCH_ID)
        mock_get_batch.assert_called_once_with(BATCH_ID)
        self.mock_context["ti"].xcom_push.assert_called_once_with(key="app_id", value=APP_ID)

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state",
        return_value=BatchState.SUCCESS,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    def test_execution_with_extra_options_deferrable(
        self, mock_get_batch, mock_post, mock_get_batch_state, mock_dump_logs
    ):
        extra_options = {"check_response": True}
        task = LivyOperator(
            file="sparkapp",
            dag=self.dag,
            task_id="livy_example",
            extra_options=extra_options,
            deferrable=True,
        )

        task.execute(context=self.mock_context)
        assert task.hook.extra_options == extra_options

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.delete_batch")
    def test_when_kill_is_called_right_after_construction_it_should_not_raise_attribute_error(
        self, mock_delete_batch
    ):
        task = LivyOperator(
            livy_conn_id="livyunittest",
            file="sparkapp",
            dag=self.dag,
            task_id="livy_example",
        )
        task.kill()
        mock_delete_batch.assert_not_called()

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.delete_batch")
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state",
        return_value=BatchState.SUCCESS,
    )
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    def test_deletion_deferrable(
        self, mock_dump_logs, mock_get_batch_state, mock_get_batch, mock_post, mock_delete
    ):
        task = LivyOperator(
            livy_conn_id="livyunittest",
            file="sparkapp",
            dag=self.dag,
            task_id="livy_example",
            deferrable=True,
        )
        task.execute(context=self.mock_context)
        task.kill()

        mock_delete.assert_called_once_with(BATCH_ID)

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value={"appId": APP_ID})
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    def test_execute_complete_success(self, mock_post, mock_get):
        task = LivyOperator(
            livy_conn_id="livyunittest",
            file="sparkapp",
            dag=self.dag,
            task_id="livy_example",
            polling_interval=1,
            deferrable=True,
        )
        result = task.execute_complete(
            context=self.mock_context,
            event={
                "status": "success",
                "log_lines": None,
                "batch_id": BATCH_ID,
                "response": "mock success",
            },
        )

        assert result == BATCH_ID
        self.mock_context["ti"].xcom_push.assert_called_once_with(key="app_id", value=APP_ID)

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    def test_execute_complete_error(self, mock_post):
        task = LivyOperator(
            livy_conn_id="livyunittest",
            file="sparkapp",
            dag=self.dag,
            task_id="livy_example",
            polling_interval=1,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context=self.mock_context,
                event={
                    "status": "error",
                    "log_lines": ["mock log"],
                    "batch_id": BATCH_ID,
                    "response": "mock error",
                },
            )
        self.mock_context["ti"].xcom_push.assert_not_called()

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.delete_batch")
    def test_execute_complete_timeout(self, mock_delete, mock_post):
        task = LivyOperator(
            livy_conn_id="livyunittest",
            file="sparkapp",
            dag=self.dag,
            task_id="livy_example",
            polling_interval=1,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context=self.mock_context,
                event={
                    "status": "timeout",
                    "log_lines": ["mock log"],
                    "batch_id": BATCH_ID,
                    "response": "mock timeout",
                },
            )
        mock_delete.assert_called_once_with(BATCH_ID)
        self.mock_context["ti"].xcom_push.assert_not_called()

    @patch.object(LivyOperator, "hook", new_callable=MagicMock)
    @patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    def test_inject_simple_openlineage_config_to_spark(self, mock_get_openlineage_listener, mock_hook):
        # Given / When
        from openlineage.client.transport.http import (
            ApiKeyTokenProvider,
            HttpCompression,
            HttpConfig,
            HttpTransport,
        )

        mock_get_openlineage_listener.return_value.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
            config=HttpConfig(
                url="http://localhost:5000",
                endpoint="api/v2/lineage",
                timeout=5050,
                auth=ApiKeyTokenProvider({"api_key": "12345"}),
                compression=HttpCompression.GZIP,
                custom_headers={"X-OpenLineage-Custom-Header": "airflow"},
            )
        )

        operator = LivyOperator(
            file="sparkapp",
            livy_conn_id="livy_default",
            polling_interval=1,
            dag=self.dag,
            task_id="livy_example",
            conf={},
            deferrable=False,
            openlineage_inject_parent_job_info=False,
            openlineage_inject_transport_info=True,
        )
        operator.hook.get_batch_state.return_value = BatchState.SUCCESS
        operator.hook.TERMINAL_STATES = [BatchState.SUCCESS]
        operator.execute(MagicMock())

        assert operator.spark_params["conf"] == {
            "spark.openlineage.transport.type": "http",
            "spark.openlineage.transport.url": "http://localhost:5000",
            "spark.openlineage.transport.endpoint": "api/v2/lineage",
            "spark.openlineage.transport.timeoutInMillis": "5050000",
            "spark.openlineage.transport.compression": "gzip",
            "spark.openlineage.transport.auth.type": "api_key",
            "spark.openlineage.transport.auth.apiKey": "Bearer 12345",
            "spark.openlineage.transport.headers.X-OpenLineage-Custom-Header": "airflow",
        }

    @patch.object(LivyOperator, "hook", new_callable=MagicMock)
    @patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_batch_state")
    @patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    def test_inject_composite_openlineage_config_to_spark(
        self, mock_get_openlineage_listener, mock_get_batch_state, mock_hook
    ):
        # Given / When
        from openlineage.client.transport.composite import CompositeConfig, CompositeTransport

        mock_get_openlineage_listener.return_value.adapter.get_or_create_openlineage_client.return_value.transport = CompositeTransport(
            CompositeConfig.from_dict(
                {
                    "transports": {
                        "test1": {
                            "type": "http",
                            "url": "http://localhost:5000",
                            "endpoint": "api/v2/lineage",
                            "timeout": 5050,
                            "auth": {
                                "type": "api_key",
                                "api_key": "12345",
                            },
                            "compression": "gzip",
                            "custom_headers": {"X-OpenLineage-Custom-Header": "airflow"},
                        },
                        "test2": {"type": "http", "url": "https://example.com:1234"},
                        "test3": {"type": "console"},
                    }
                }
            )
        )

        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag_id"
        mock_ti.task_id = "spark_submit_job"
        mock_ti.try_number = 1
        mock_ti.dag_run.logical_date = DEFAULT_DATE
        mock_ti.dag_run.run_after = DEFAULT_DATE
        mock_ti.dag_run.clear_number = 0
        mock_ti.logical_date = DEFAULT_DATE
        mock_ti.map_index = -1
        mock_get_batch_state.return_value = BatchState.SUCCESS

        operator = LivyOperator(
            file="sparkapp",
            livy_conn_id="spark_default",
            polling_interval=1,
            dag=self.dag,
            task_id="livy_example",
            deferrable=False,
            openlineage_inject_parent_job_info=True,
            openlineage_inject_transport_info=True,
        )
        operator.hook.get_batch_state.return_value = BatchState.SUCCESS
        operator.hook.TERMINAL_STATES = [BatchState.SUCCESS]

        operator.execute({"ti": mock_ti})

        assert operator.spark_params["conf"] == {
            "spark.openlineage.parentJobName": "test_dag_id.spark_submit_job",
            "spark.openlineage.parentJobNamespace": "default",
            "spark.openlineage.parentRunId": "01595753-6400-710b-8a12-9e978335a56d",
            "spark.openlineage.rootParentJobName": "test_dag_id",
            "spark.openlineage.rootParentJobNamespace": "default",
            "spark.openlineage.rootParentRunId": "01595753-6400-71fe-a08c-aaed126ab6fb",
            "spark.openlineage.transport.type": "composite",
            "spark.openlineage.transport.continueOnFailure": "True",
            "spark.openlineage.transport.transports.test1.type": "http",
            "spark.openlineage.transport.transports.test1.url": "http://localhost:5000",
            "spark.openlineage.transport.transports.test1.endpoint": "api/v2/lineage",
            "spark.openlineage.transport.transports.test1.timeoutInMillis": "5050000",
            "spark.openlineage.transport.transports.test1.auth.type": "api_key",
            "spark.openlineage.transport.transports.test1.auth.apiKey": "Bearer 12345",
            "spark.openlineage.transport.transports.test1.compression": "gzip",
            "spark.openlineage.transport.transports.test1.headers.X-OpenLineage-Custom-Header": "airflow",
            "spark.openlineage.transport.transports.test2.type": "http",
            "spark.openlineage.transport.transports.test2.url": "https://example.com:1234",
            "spark.openlineage.transport.transports.test2.endpoint": "api/v1/lineage",
            "spark.openlineage.transport.transports.test2.timeoutInMillis": "5000",
        }

    @patch.object(LivyOperator, "hook", new_callable=MagicMock)
    @patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_batch_state")
    @patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    def test_inject_openlineage_composite_config_wrong_transport_to_spark(
        self, mock_get_openlineage_listener, mock_get_batch_state, mock_hook, caplog
    ):
        # Given / When
        from openlineage.client.transport.composite import CompositeConfig, CompositeTransport

        mock_get_openlineage_listener.return_value.adapter.get_or_create_openlineage_client.return_value.transport = CompositeTransport(
            CompositeConfig.from_dict({"transports": {"test1": {"type": "console"}}})
        )
        mock_get_batch_state.return_value = BatchState.SUCCESS

        with caplog.at_level(logging.INFO):
            operator = LivyOperator(
                file="sparkapp",
                livy_conn_id="livy_default",
                polling_interval=1,
                dag=self.dag,
                task_id="livy_example",
                deferrable=False,
                openlineage_inject_parent_job_info=False,
                openlineage_inject_transport_info=True,
            )
            operator.hook.get_batch_state.return_value = BatchState.SUCCESS
            operator.hook.TERMINAL_STATES = [BatchState.SUCCESS]
            operator.execute(MagicMock())

            assert (
                "OpenLineage transport type `composite` does not contain http transport. Skipping injection of OpenLineage transport information into Spark properties."
                in caplog.text
            )
        assert operator.spark_params["conf"] == {}

    @patch.object(LivyOperator, "hook", new_callable=MagicMock)
    @patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_batch_state")
    @patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    def test_inject_openlineage_simple_config_wrong_transport_to_spark(
        self, mock_get_openlineage_listener, mock_get_batch_state, mock_hook, caplog
    ):
        # Given / When
        from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

        mock_get_openlineage_listener.return_value.adapter.get_or_create_openlineage_client.return_value.transport = ConsoleTransport(
            config=ConsoleConfig()
        )
        mock_hook.get_batch_state.return_value = BatchState.SUCCESS

        with caplog.at_level(logging.INFO):
            operator = LivyOperator(
                file="sparkapp",
                livy_conn_id="livy_default",
                polling_interval=1,
                dag=self.dag,
                task_id="livy_example",
                deferrable=False,
                openlineage_inject_parent_job_info=False,
                openlineage_inject_transport_info=True,
            )
            operator.hook.get_batch_state.return_value = BatchState.SUCCESS
            operator.hook.TERMINAL_STATES = [BatchState.SUCCESS]
            operator.execute(MagicMock())

            assert (
                "OpenLineage transport type `console` does not support automatic injection of OpenLineage transport information into Spark properties."
                in caplog.text
            )
        assert operator.spark_params["conf"] == {}


@pytest.mark.db_test
def test_spark_params_templating(create_task_instance_of_operator, session):
    ti = create_task_instance_of_operator(
        LivyOperator,
        # Templated fields
        file="{{ 'literal-file' }}",
        class_name="{{ 'literal-class-name' }}",
        args="{{ 'literal-args' }}",
        jars="{{ 'literal-jars' }}",
        py_files="{{ 'literal-py-files' }}",
        files="{{ 'literal-files' }}",
        driver_memory="{{ 'literal-driver-memory' }}",
        driver_cores="{{ 'literal-driver-cores' }}",
        executor_memory="{{ 'literal-executor-memory' }}",
        executor_cores="{{ 'literal-executor-cores' }}",
        num_executors="{{ 'literal-num-executors' }}",
        archives="{{ 'literal-archives' }}",
        queue="{{ 'literal-queue' }}",
        name="{{ 'literal-name' }}",
        conf="{{ 'literal-conf' }}",
        proxy_user="{{ 'literal-proxy-user' }}",
        # Other parameters
        dag_id="test_template_body_templating_dag",
        task_id="test_template_body_templating_task",
    )
    session.add(ti)
    session.commit()
    ti.render_templates()
    task: LivyOperator = ti.task
    assert task.spark_params == {
        "archives": "literal-archives",
        "args": "literal-args",
        "class_name": "literal-class-name",
        "conf": "literal-conf",
        "driver_cores": "literal-driver-cores",
        "driver_memory": "literal-driver-memory",
        "executor_cores": "literal-executor-cores",
        "executor_memory": "literal-executor-memory",
        "file": "literal-file",
        "files": "literal-files",
        "jars": "literal-jars",
        "name": "literal-name",
        "num_executors": "literal-num-executors",
        "proxy_user": "literal-proxy-user",
        "py_files": "literal-py-files",
        "queue": "literal-queue",
    }
