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

import logging
import warnings
from datetime import timedelta
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.taskinstance import create_task_instance, render_template_fields
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_3_PLUS

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSparkSubmitOperator:
    _config = {
        "conf": {"parquet.compression": "SNAPPY"},
        "files": "hive-site.xml",
        "py_files": "sample_library.py",
        "archives": "sample_archive.zip#SAMPLE",
        "driver_class_path": "parquet.jar",
        "jars": "parquet.jar",
        "packages": "com.databricks:spark-avro_2.11:3.2.0",
        "exclude_packages": "org.bad.dependency:1.0.0",
        "repositories": "http://myrepo.org",
        "total_executor_cores": 4,
        "executor_cores": 4,
        "executor_memory": "22g",
        "keytab": "privileged_user.keytab",
        "principal": "user/spark@airflow.org",
        "proxy_user": "sample_user",
        "name": "{{ task_instance.task_id }}",
        "num_executors": 10,
        "status_poll_interval": 30,
        "verbose": True,
        "application": "test_application.py",
        "driver_memory": "3g",
        "java_class": "com.foo.bar.AppMain",
        "properties_file": "conf/spark-custom.conf",
        "application_args": [
            "-f",
            "foo",
            "--bar",
            "bar",
            "--start",
            "{{ macros.ds_add(ds, -1)}}",
            "--end",
            "{{ ds }}",
            "--with-spaces",
            "args should keep embedded spaces",
        ],
        "use_krb5ccache": True,
        "yarn_queue": "yarn_dev_queue2",
        "deploy_mode": "client2",
        "queue": "airflow_custom_queue",
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

    def test_execute(self):
        # Given / When
        conn_id = "spark_default"
        operator = SparkSubmitOperator(
            task_id="spark_submit_job",
            spark_binary="sparky",
            dag=self.dag,
            **self._config,
        )

        # Then expected results
        expected_dict = {
            "conf": {"parquet.compression": "SNAPPY"},
            "files": "hive-site.xml",
            "py_files": "sample_library.py",
            "archives": "sample_archive.zip#SAMPLE",
            "driver_class_path": "parquet.jar",
            "jars": "parquet.jar",
            "packages": "com.databricks:spark-avro_2.11:3.2.0",
            "exclude_packages": "org.bad.dependency:1.0.0",
            "repositories": "http://myrepo.org",
            "total_executor_cores": 4,
            "executor_cores": 4,
            "executor_memory": "22g",
            "keytab": "privileged_user.keytab",
            "principal": "user/spark@airflow.org",
            "proxy_user": "sample_user",
            "name": "{{ task_instance.task_id }}",
            "num_executors": 10,
            "status_poll_interval": 30,
            "verbose": True,
            "application": "test_application.py",
            "driver_memory": "3g",
            "java_class": "com.foo.bar.AppMain",
            "application_args": [
                "-f",
                "foo",
                "--bar",
                "bar",
                "--start",
                "{{ macros.ds_add(ds, -1)}}",
                "--end",
                "{{ ds }}",
                "--with-spaces",
                "args should keep embedded spaces",
            ],
            "spark_binary": "sparky",
            "yarn_queue": "yarn_dev_queue2",
            "deploy_mode": "client2",
            "use_krb5ccache": True,
            "properties_file": "conf/spark-custom.conf",
            "queue": "airflow_custom_queue",
        }

        assert conn_id == operator._conn_id
        assert expected_dict["application"] == operator.application
        assert expected_dict["conf"] == operator.conf
        assert expected_dict["files"] == operator.files
        assert expected_dict["py_files"] == operator.py_files
        assert expected_dict["archives"] == operator._archives
        assert expected_dict["driver_class_path"] == operator.driver_class_path
        assert expected_dict["jars"] == operator.jars
        assert expected_dict["packages"] == operator.packages
        assert expected_dict["exclude_packages"] == operator.exclude_packages
        assert expected_dict["repositories"] == operator._repositories
        assert expected_dict["total_executor_cores"] == operator._total_executor_cores
        assert expected_dict["executor_cores"] == operator._executor_cores
        assert expected_dict["executor_memory"] == operator._executor_memory
        assert expected_dict["keytab"] == operator.keytab
        assert expected_dict["principal"] == operator.principal
        assert expected_dict["proxy_user"] == operator.proxy_user
        assert expected_dict["name"] == operator.name
        assert expected_dict["num_executors"] == operator._num_executors
        assert expected_dict["status_poll_interval"] == operator._status_poll_interval
        assert expected_dict["verbose"] == operator._verbose
        assert expected_dict["java_class"] == operator._java_class
        assert expected_dict["driver_memory"] == operator._driver_memory
        assert expected_dict["application_args"] == operator.application_args
        assert expected_dict["spark_binary"] == operator._spark_binary
        assert expected_dict["deploy_mode"] == operator._deploy_mode
        assert expected_dict["properties_file"] == operator.properties_file
        assert expected_dict["use_krb5ccache"] == operator._use_krb5ccache
        assert expected_dict["queue"] == operator.queue
        assert expected_dict["yarn_queue"] == operator._yarn_queue

    @pytest.mark.db_test
    def test_spark_submit_cmd_connection_overrides(self):
        config = self._config
        # have to add this otherwise we can't run
        # _build_spark_submit_command
        config["use_krb5ccache"] = False
        operator = SparkSubmitOperator(
            task_id="spark_submit_job", spark_binary="sparky", dag=self.dag, **config
        )
        cmd = " ".join(operator._get_hook()._build_spark_submit_command("test"))
        assert "--queue yarn_dev_queue2" in cmd  # yarn queue
        assert "--deploy-mode client2" in cmd
        assert "sparky" in cmd
        assert operator.queue == "airflow_custom_queue"  # airflow queue

        # if we don't pass any overrides in arguments, default values
        config["yarn_queue"] = None
        config["deploy_mode"] = None
        config.pop("queue", None)  # using default airflow queue
        operator2 = SparkSubmitOperator(task_id="spark_submit_job2", dag=self.dag, **config)
        cmd2 = " ".join(operator2._get_hook()._build_spark_submit_command("test"))
        assert "--queue root.default" in cmd2  # yarn queue
        assert "--deploy-mode client2" not in cmd2
        assert "spark-submit" in cmd2
        assert operator2.queue == "default"  # airflow queue

    @pytest.mark.db_test
    def test_render_template(self, session, testing_dag_bundle):
        # Given
        operator = SparkSubmitOperator(task_id="spark_submit_job", dag=self.dag, **self._config)

        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion

            sync_dag_to_db(self.dag)
            dag_version = DagVersion.get_latest_version(operator.dag_id)
            ti = create_task_instance(operator, run_id="spark_test", dag_version_id=dag_version.id)
            ti.dag_run = DagRun(
                dag_id=self.dag.dag_id,
                run_id="spark_test",
                logical_date=DEFAULT_DATE,
                data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                run_after=DEFAULT_DATE,
                run_type=DagRunType.MANUAL,
                state="running",
            )
        else:
            ti = TaskInstance(operator, run_id="spark_test")
            ti.dag_run = DagRun(
                dag_id=self.dag.dag_id,
                run_id="spark_test",
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.MANUAL,
                state="running",
            )

        session.add(ti)
        session.commit()
        # When
        render_template_fields(ti, operator)

        # Then
        expected_application_args = [
            "-f",
            "foo",
            "--bar",
            "bar",
            "--start",
            (DEFAULT_DATE - timedelta(days=1)).strftime("%Y-%m-%d"),
            "--end",
            DEFAULT_DATE.strftime("%Y-%m-%d"),
            "--with-spaces",
            "args should keep embedded spaces",
        ]
        expected_name = "spark_submit_job"
        assert expected_application_args == getattr(operator, "application_args")
        assert expected_name == getattr(operator, "name")

    @pytest.mark.db_test
    def test_templating_with_create_task_instance_of_operator(
        self, create_task_instance_of_operator, session
    ):
        ti = create_task_instance_of_operator(
            SparkSubmitOperator,
            # Templated fields
            application="{{ 'application' }}",
            conf="{{ 'conf' }}",
            files="{{ 'files' }}",
            py_files="{{ 'py-files' }}",
            jars="{{ 'jars' }}",
            driver_class_path="{{ 'driver_class_path' }}",
            packages="{{ 'packages' }}",
            exclude_packages="{{ 'exclude_packages' }}",
            keytab="{{ 'keytab' }}",
            principal="{{ 'principal' }}",
            proxy_user="{{ 'proxy_user' }}",
            name="{{ 'name' }}",
            application_args="{{ 'application_args' }}",
            env_vars="{{ 'env_vars' }}",
            properties_file="{{ 'properties_file' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
        )
        session.add(ti)
        session.commit()
        task = ti.render_templates()
        assert task.application == "application"
        assert task.conf == "conf"
        assert task.files == "files"
        assert task.py_files == "py-files"
        assert task.jars == "jars"
        assert task.driver_class_path == "driver_class_path"
        assert task.packages == "packages"
        assert task.exclude_packages == "exclude_packages"
        assert task.keytab == "keytab"
        assert task.principal == "principal"
        assert task.proxy_user == "proxy_user"
        assert task.name == "name"
        assert task.application_args == "application_args"
        assert task.env_vars == "env_vars"
        assert task.properties_file == "properties_file"

    @mock.patch.object(SparkSubmitOperator, "_get_hook")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    def test_inject_simple_openlineage_config_to_spark(self, mock_get_openlineage_listener, mock_get_hook):
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
        operator = SparkSubmitOperator(
            task_id="spark_submit_job",
            spark_binary="sparky",
            dag=self.dag,
            openlineage_inject_parent_job_info=False,
            openlineage_inject_transport_info=True,
            **self._config,
        )
        mock_get_hook.return_value._should_track_driver_status = False
        mock_get_hook.return_value._should_track_driver_via_k8s_api.return_value = False
        operator.execute(MagicMock())

        assert operator.conf == {
            "parquet.compression": "SNAPPY",
            "spark.openlineage.transport.type": "http",
            "spark.openlineage.transport.url": "http://localhost:5000",
            "spark.openlineage.transport.endpoint": "api/v2/lineage",
            "spark.openlineage.transport.timeoutInMillis": "5050000",
            "spark.openlineage.transport.compression": "gzip",
            "spark.openlineage.transport.auth.type": "api_key",
            "spark.openlineage.transport.auth.apiKey": "Bearer 12345",
            "spark.openlineage.transport.headers.X-OpenLineage-Custom-Header": "airflow",
        }

    @mock.patch.object(SparkSubmitOperator, "_get_hook")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    def test_inject_composite_openlineage_config_to_spark(self, mock_get_openlineage_listener, mock_get_hook):
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
                            "timeout": "5050",
                            "auth": {
                                "type": "api_key",
                                "api_key": "12345",
                            },
                            "compression": "gzip",
                            "custom_headers": {
                                "X-OpenLineage-Custom-Header": "airflow",
                            },
                        },
                        "test2": {
                            "type": "http",
                            "url": "https://example.com:1234",
                        },
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

        operator = SparkSubmitOperator(
            task_id="spark_submit_job",
            spark_binary="sparky",
            dag=self.dag,
            openlineage_inject_parent_job_info=True,
            openlineage_inject_transport_info=True,
            **self._config,
        )
        mock_get_hook.return_value._should_track_driver_status = False
        mock_get_hook.return_value._should_track_driver_via_k8s_api.return_value = False
        operator.execute({"ti": mock_ti})

        assert operator.conf == {
            "parquet.compression": "SNAPPY",
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

    @mock.patch.object(SparkSubmitOperator, "_get_hook")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    def test_inject_openlineage_composite_config_wrong_transport_to_spark(
        self, mock_get_openlineage_listener, mock_get_hook, caplog
    ):
        # Given / When
        from openlineage.client.transport.composite import CompositeConfig, CompositeTransport

        mock_get_openlineage_listener.return_value.adapter.get_or_create_openlineage_client.return_value.transport = CompositeTransport(
            CompositeConfig.from_dict({"transports": {"test1": {"type": "console"}}})
        )

        mock_get_hook.return_value._should_track_driver_status = False
        mock_get_hook.return_value._should_track_driver_via_k8s_api.return_value = False
        with caplog.at_level(logging.INFO):
            operator = SparkSubmitOperator(
                task_id="spark_submit_job",
                spark_binary="sparky",
                dag=self.dag,
                openlineage_inject_parent_job_info=False,
                openlineage_inject_transport_info=True,
                **self._config,
            )
            operator.execute(MagicMock())

            assert (
                "OpenLineage transport type `composite` does not contain http transport. Skipping injection of OpenLineage transport information into Spark properties."
                in caplog.text
            )
        assert operator.conf == {
            "parquet.compression": "SNAPPY",
        }

    @mock.patch.object(SparkSubmitOperator, "_get_hook")
    @mock.patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
    def test_inject_openlineage_simple_config_wrong_transport_to_spark(
        self, mock_get_openlineage_listener, mock_get_hook, caplog
    ):
        # Given / When
        from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

        mock_get_openlineage_listener.return_value.adapter.get_or_create_openlineage_client.return_value.transport = ConsoleTransport(
            config=ConsoleConfig()
        )

        mock_get_hook.return_value._should_track_driver_status = False
        mock_get_hook.return_value._should_track_driver_via_k8s_api.return_value = False
        with caplog.at_level(logging.INFO):
            operator = SparkSubmitOperator(
                task_id="spark_submit_job",
                spark_binary="sparky",
                dag=self.dag,
                openlineage_inject_parent_job_info=False,
                openlineage_inject_transport_info=True,
                **self._config,
            )
            operator.execute(MagicMock())

            assert (
                "OpenLineage transport type `console` does not support automatic injection of OpenLineage transport information into Spark properties."
                in caplog.text
            )
        assert operator.conf == {
            "parquet.compression": "SNAPPY",
        }


class FakeTaskStateStore:
    """In-memory task state store for tests."""

    def __init__(self, stored: dict[str, str] | None = None):
        self._store: dict[str, str] = dict(stored or {})

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def set(self, key: str, value: str) -> None:
        self._store[key] = value


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS,
    reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
)
class TestSparkSubmitOperatorResumable:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_resumable_dag", schedule=None, default_args=args)

    def _make_operator(self, **kwargs):
        return SparkSubmitOperator(task_id="test", dag=self.dag, application="test.jar", **kwargs)

    def _make_hook(self, should_track=False, is_yarn=False, is_yarn_cluster=False, is_kubernetes=False):
        hook = MagicMock()
        hook._should_track_driver_status = should_track
        hook._should_track_driver_via_k8s_api.return_value = False
        hook._is_yarn = is_yarn
        hook._is_yarn_cluster_mode = is_yarn_cluster
        hook._is_kubernetes = is_kubernetes
        hook._connection = {"master": "spark://localhost:7077"}
        return hook

    def test_non_cluster_mode_calls_hook_submit_directly(self):
        operator = self._make_operator()
        operator._hook = self._make_hook(should_track=False)

        operator.execute(context={})

        operator._hook.submit.assert_called_once_with("test.jar")

    def test_cluster_mode_first_run_persists_id_before_polling(self):
        operator = self._make_operator()
        operator._hook = self._make_hook(should_track=True)
        operator._hook.submit.return_value = "driver-001"

        task_store = FakeTaskStateStore()
        persisted_before_poll = []

        def track_poll(external_id, context):
            persisted_before_poll.append(task_store.get("spark_job_id"))

        operator.poll_until_complete = track_poll

        operator.execute(context={"task_state_store": task_store})

        operator._hook.submit.assert_called_once_with("test.jar")
        assert persisted_before_poll == ["driver-001"]

    @pytest.mark.parametrize(
        ("prior_status", "expect_submit", "expect_poll_id"),
        [
            ("RUNNING", False, "driver-001"),
            ("SUBMITTED", False, "driver-001"),
            ("FINISHED", False, None),
            ("FAILED", True, "driver-new"),
            ("KILLED", True, "driver-new"),
        ],
    )
    def test_retry_behaviour_based_on_prior_driver_status(self, prior_status, expect_submit, expect_poll_id):
        operator = self._make_operator()
        operator._hook = self._make_hook(should_track=True)
        operator._hook.submit.return_value = "driver-new"
        task_store = FakeTaskStateStore({"spark_job_id": "driver-001"})

        operator.get_job_status = lambda external_id, context: prior_status
        polled = []
        operator.poll_until_complete = lambda external_id, context: polled.append(external_id)

        operator.execute(context={"task_state_store": task_store})

        if expect_submit:
            operator._hook.submit.assert_called_once_with("test.jar")
        else:
            operator._hook.submit.assert_not_called()

        if expect_poll_id:
            assert polled == [expect_poll_id]
        else:
            assert polled == []

    def test_submits_fresh_when_task_state_store_unavailable(self):
        operator = self._make_operator()
        operator._hook = self._make_hook(should_track=True)
        operator._hook.submit.return_value = "driver-001"
        polled = []
        operator.poll_until_complete = lambda external_id, context: polled.append(external_id)

        # no task_state_store key in context
        operator.execute(context={})

        operator._hook.submit.assert_called_once_with("test.jar")
        assert polled == ["driver-001"]

    def test_reconnect_on_retry_deprecated_alias(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            operator = self._make_operator(reconnect_on_retry=False)
        assert len(w) == 1
        assert issubclass(w[0].category, AirflowProviderDeprecationWarning)
        assert "reconnect_on_retry" in str(w[0].message)
        assert operator.durable is False

    def test_durable_false_submits_fresh_and_polls(self):
        operator = self._make_operator(durable=False)
        operator._hook = self._make_hook(should_track=True)
        operator._hook.submit.return_value = "driver-new"
        task_store = FakeTaskStateStore({"spark_job_id": "driver-old"})
        polled = []
        operator.poll_until_complete = lambda external_id, context: polled.append(external_id)

        operator.execute(context={"task_state_store": task_store})
        # durable=False: ignores prior driver ID, submits fresh, but still polls
        operator._hook.submit.assert_called_once_with("test.jar")
        assert polled == ["driver-new"]

    @pytest.mark.parametrize(
        ("is_yarn_cluster", "is_kubernetes", "status", "expected_active", "expected_succeeded"),
        [
            # Spark standalone cluster mode
            (False, False, "RUNNING", True, False),
            (False, False, "SUBMITTED", True, False),
            (False, False, "RELAUNCHING", True, False),
            (False, False, "UNKNOWN", True, False),
            (False, False, "FINISHED", False, True),
            (False, False, "FAILED", False, False),
            # YARN cluster mode — synthesized statuses from query_yarn_application_status
            (True, False, "NEW", True, False),
            (True, False, "NEW_SAVING", True, False),
            (True, False, "SUBMITTED", True, False),
            (True, False, "ACCEPTED", True, False),
            (True, False, "RUNNING", True, False),
            (True, False, "SUCCEEDED", False, True),
            (True, False, "FAILED", False, False),
            # Kubernetes
            (False, True, "Running", True, False),
            (False, True, "Pending", True, False),
            (False, True, "Succeeded", False, True),
            (False, True, "Failed", False, False),
        ],
    )
    def test_job_status_mappings(
        self, is_yarn_cluster, is_kubernetes, status, expected_active, expected_succeeded
    ):
        operator = self._make_operator()
        operator._hook = self._make_hook(is_yarn_cluster=is_yarn_cluster, is_kubernetes=is_kubernetes)

        assert operator.is_job_active(status) == expected_active
        assert operator.is_job_succeeded(status) == expected_succeeded

    @pytest.mark.parametrize(
        ("response_json", "expected_status", "expected_error"),
        [
            ({"success": True, "driverState": "RUNNING"}, "RUNNING", None),
            ({"success": False, "message": "driver not found"}, None, "driver not found"),
            ({"driverState": "RUNNING"}, None, "unknown error"),
        ],
    )
    def test_get_job_status(self, response_json, expected_status, expected_error):
        operator = self._make_operator()
        operator._hook = self._make_hook(should_track=True)
        mock_response = MagicMock()
        mock_response.json.return_value = response_json

        with mock.patch("requests.get", return_value=mock_response):
            if expected_error:
                with pytest.raises(RuntimeError, match=expected_error):
                    operator.get_job_status("driver-001", {})
            else:
                assert operator.get_job_status("driver-001", {}) == expected_status

    def test_get_job_status_ha_tries_next_master(self):
        operator = self._make_operator()
        hook = self._make_hook(should_track=True)
        # Master URL port (7077) is RPC — REST API must use 6066, not 7077
        hook._connection = {"master": "spark://m1:7077,m2:7077"}
        operator._hook = hook

        good_response = MagicMock()
        good_response.json.return_value = {"success": True, "driverState": "RUNNING"}
        captured_urls = []

        def side_effect(url, timeout):
            captured_urls.append(url)
            if "m1" in url:
                raise ConnectionError("m1 unreachable")
            return good_response

        with mock.patch("requests.get", side_effect=side_effect):
            assert operator.get_job_status("driver-001", {}) == "RUNNING"

        assert all(":6066/" in url for url in captured_urls), "REST API must use port 6066, not the RPC port"

    def test_get_job_status_ha_tries_next_master_on_success_false(self):
        """success:false from m1 (e.g. HA recovery in progress) should fall through to m2."""
        operator = self._make_operator()
        hook = self._make_hook(should_track=True)
        hook._connection = {"master": "spark://m1:7077,m2:7077"}
        operator._hook = hook

        bad_response = MagicMock()
        bad_response.json.return_value = {"success": False, "message": "Driver not found"}
        good_response = MagicMock()
        good_response.json.return_value = {"success": True, "driverState": "RUNNING"}

        def side_effect(url, timeout):
            if "m1" in url:
                return bad_response
            return good_response

        with mock.patch("requests.get", side_effect=side_effect):
            assert operator.get_job_status("driver-001", {}) == "RUNNING"

    def test_get_job_status_ha_raises_when_all_masters_unreachable(self):
        operator = self._make_operator()
        hook = self._make_hook(should_track=True)
        hook._connection = {"master": "spark://m1:7077,m2:7077"}
        operator._hook = hook

        with mock.patch("requests.get", side_effect=ConnectionError("unreachable")):
            with pytest.raises(ConnectionError):
                operator.get_job_status("driver-001", {})

    def test_get_job_status_uses_rest_scheme_from_connection(self):
        operator = self._make_operator()
        hook = self._make_hook(should_track=True)
        hook._connection = {"master": "spark://myhost:6066", "rest_scheme": "https"}
        operator._hook = hook

        mock_response = MagicMock()
        mock_response.json.return_value = {"success": True, "driverState": "RUNNING"}
        captured_urls = []

        def capture(url, timeout):
            captured_urls.append(url)
            return mock_response

        with mock.patch("requests.get", side_effect=capture):
            operator.get_job_status("driver-001", {})

        assert len(captured_urls) == 1
        assert captured_urls[0].startswith("https://")

    def test_yarn_first_run_persists_app_id_before_polling(self):
        operator = self._make_operator()
        operator._hook = self._make_hook(is_yarn_cluster=True)
        operator._hook._conf = {}
        operator._hook._yarn_application_id = "application_1234_0001"
        operator._hook.submit.return_value = None

        task_store = FakeTaskStateStore()
        persisted_before_poll = []

        def track_poll(external_id, context):
            persisted_before_poll.append(task_store.get("spark_job_id"))

        operator.poll_until_complete = track_poll
        operator.execute(context={"task_state_store": task_store})

        assert persisted_before_poll == ["application_1234_0001"]

    def test_yarn_retry_reconnects_to_running_app(self):
        operator = self._make_operator()
        operator._hook = self._make_hook(is_yarn_cluster=True)
        task_store = FakeTaskStateStore({"spark_job_id": "application_1234_0001"})

        operator.get_job_status = lambda external_id, context: "RUNNING"
        polled = []
        operator.poll_until_complete = lambda external_id, context: polled.append(external_id)

        operator.execute(context={"task_state_store": task_store})

        operator._hook.submit.assert_not_called()
        assert polled == ["application_1234_0001"]

    def test_yarn_retry_skips_already_succeeded_app(self):
        operator = self._make_operator()
        operator._hook = self._make_hook(is_yarn_cluster=True)
        task_store = FakeTaskStateStore({"spark_job_id": "application_1234_0001"})

        operator.get_job_status = lambda external_id, context: "SUCCEEDED"

        operator.execute(context={"task_state_store": task_store})

        operator._hook.submit.assert_not_called()

    def test_yarn_retry_resubmits_after_failed_app(self):
        operator = self._make_operator()
        operator._hook = self._make_hook(is_yarn_cluster=True)
        operator._hook._conf = {}
        operator._hook._yarn_application_id = "application_1234_0002"
        operator._hook.submit.return_value = None
        task_store = FakeTaskStateStore({"spark_job_id": "application_1234_0001"})

        operator.get_job_status = lambda external_id, context: "FAILED"
        polled = []
        operator.poll_until_complete = lambda external_id, context: polled.append(external_id)

        operator.execute(context={"task_state_store": task_store})

        operator._hook.submit.assert_called_once_with("test.jar")
        assert polled == ["application_1234_0002"]

    def test_yarn_injects_wait_app_completion_false(self):
        operator = self._make_operator()
        hook = self._make_hook(is_yarn_cluster=True)
        hook._conf = {}
        hook._yarn_application_id = "application_1234_0001"
        hook.submit.return_value = None
        operator._hook = hook

        operator.submit_job(context={})

        assert hook._conf.get("spark.yarn.submit.waitAppCompletion") == "false"

    def test_yarn_raises_if_wait_app_completion_true(self):
        operator = self._make_operator()
        hook = self._make_hook(is_yarn_cluster=True)
        hook._conf = {"spark.yarn.submit.waitAppCompletion": "true"}
        operator._hook = hook

        with pytest.raises(ValueError, match="waitAppCompletion=true"):
            operator.submit_job(context={})

    def test_yarn_poll_tolerates_transient_resourcemanager_failures(self):
        operator = self._make_operator()
        operator._hook = self._make_hook(is_yarn_cluster=True)
        operator._hook._status_poll_interval = 0

        call_count = 0

        def flaky_status(external_id):
            nonlocal call_count
            call_count += 1
            if call_count < 5:
                raise RuntimeError("RM temporarily unavailable")
            return "SUCCEEDED"

        operator.get_job_status = flaky_status

        with mock.patch("time.sleep"):
            operator.poll_until_complete("application_1234_0001", context={})

        operator._hook._run_post_submit_commands.assert_called_once()

    def test_poll_until_complete_runs_post_submit_on_failure(self):
        """post_submit_commands must run even when the driver exits with a failure status."""
        operator = self._make_operator()
        hook = self._make_hook(should_track=True)
        hook._connection = {"master": "spark://myhost:7077"}
        hook._driver_status = "FAILED"

        def simulate_failed_tracking():
            hook._driver_status = "FAILED"

        hook._start_driver_status_tracking = mock.MagicMock(side_effect=simulate_failed_tracking)
        post_submit_called = []
        hook._run_post_submit_commands = mock.MagicMock(side_effect=lambda: post_submit_called.append(True))
        operator._hook = hook

        with pytest.raises(RuntimeError, match="FAILED"):
            operator.poll_until_complete("driver-001", {})

    def test_on_kill_sends_authenticated_kill_to_yarn_rm(self):
        """operator.on_kill() must call _kill_yarn_application so Kerberos auth is applied."""
        operator = self._make_operator()
        hook = self._make_hook(is_yarn_cluster=True)
        hook._is_yarn_cluster_mode = True
        hook._yarn_application_id = "application_1234_0001"
        operator._hook = hook

        operator.on_kill()

        hook._kill_yarn_application.assert_called_once_with("application_1234_0001")

    def test_yarn_cluster_reconnect_without_rm_api_raises(self):
        """durable=True + yarn_track_via_rm_api=False must raise - RM API is required for resume."""
        operator = self._make_operator(durable=True)
        hook = self._make_hook(is_yarn_cluster=True)
        hook._yarn_track_via_rm_api = False
        operator._hook = hook

        with pytest.raises(ValueError, match="yarn_track_via_rm_api=True"):
            operator.execute(context={})

    def test_yarn_cluster_without_rm_api_reconnect_false_falls_through_to_hook_submit(self):
        """durable=False + yarn_track_via_rm_api=False falls through to hook.submit() - no RM polling."""
        operator = self._make_operator(durable=False)
        hook = self._make_hook(is_yarn_cluster=True)
        hook._yarn_track_via_rm_api = False
        operator._hook = hook

        operator.execute(context={})

        hook.submit.assert_called_once_with("test.jar")


class TestSparkSubmitOperatorK8sTracking:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_k8s_tracking_dag", schedule=None, default_args=args)

    def _make_operator(self, **kwargs):
        return SparkSubmitOperator(task_id="test", dag=self.dag, application="test.jar", **kwargs)

    def _make_k8s_hook(self):
        hook = MagicMock()
        hook._should_track_driver_status = False
        hook._should_track_driver_via_k8s_api.return_value = True
        hook._is_kubernetes = True
        hook._is_yarn = False
        hook._is_yarn_cluster_mode = False
        hook._conf = {}
        return hook

    def test_execute_calls_submit_then_poll_when_flag_set(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        hook = self._make_k8s_hook()
        operator._hook = hook
        call_order = []
        hook.submit.side_effect = lambda *a, **kw: call_order.append("submit")
        hook._poll_k8s_driver_via_api.side_effect = lambda: call_order.append("poll")

        operator.execute(context={})

        hook.submit.assert_called_once_with("test.jar")
        hook._poll_k8s_driver_via_api.assert_called_once()
        assert call_order == ["submit", "poll"]

    def test_execute_falls_through_to_plain_submit_when_flag_off(self):
        operator = self._make_operator(track_driver_via_k8s_api=False)
        hook = MagicMock()
        hook._should_track_driver_status = False
        hook._should_track_driver_via_k8s_api.return_value = False
        operator._hook = hook

        operator.execute(context={})

        hook.submit.assert_called_once_with("test.jar")
        hook._poll_k8s_driver_via_api.assert_not_called()

    def test_k8s_submit_job_returns_encoded_external_id(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        hook = self._make_k8s_hook()
        hook._kubernetes_driver_pod = "spark-abc-driver"
        hook._connection = {"namespace": "mynamespace"}
        operator._hook = hook

        result = operator.submit_job(context={})

        assert result == "mynamespace:spark-abc-driver"
        assert hook._conf.get("spark.kubernetes.submission.waitAppCompletion") == "false"
        hook.submit.assert_called_once_with("test.jar")

    def test_k8s_submit_job_raises_when_pod_name_missing(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        hook = self._make_k8s_hook()
        hook._kubernetes_driver_pod = None
        hook._connection = {"namespace": "mynamespace"}
        operator._hook = hook

        with pytest.raises(RuntimeError, match="did not capture a K8s driver pod name"):
            operator.submit_job(context={})

    def test_k8s_get_job_status_returns_k8s_driver_status(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        operator._hook = self._make_k8s_hook()
        task_store = FakeTaskStateStore({"k8s_driver_status": "Succeeded"})

        with mock.patch("airflow.providers.apache.spark.operators.spark_submit.kube_client") as mock_kube:
            result = operator.get_job_status("mynamespace:spark-abc-driver", {"task_state_store": task_store})

        assert result == "Succeeded"
        mock_kube.get_kube_client.assert_not_called()

    def test_k8s_get_job_status_queries_k8s_api_when_no_k8s_driver_status(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        operator._hook = self._make_k8s_hook()
        task_store = FakeTaskStateStore()

        mock_pod = MagicMock()
        mock_pod.status.phase = "Running"

        with mock.patch("airflow.providers.apache.spark.operators.spark_submit.kube_client") as mock_kube:
            mock_kube.get_kube_client.return_value.read_namespaced_pod.return_value = mock_pod
            result = operator.get_job_status("mynamespace:spark-abc-driver", {"task_state_store": task_store})

        assert result == "Running"

    def test_k8s_get_job_status_returns_pending_when_phase_is_none(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        operator._hook = self._make_k8s_hook()

        mock_pod = MagicMock()
        mock_pod.status.phase = None

        with mock.patch("airflow.providers.apache.spark.operators.spark_submit.kube_client") as mock_kube:
            mock_kube.get_kube_client.return_value.read_namespaced_pod.return_value = mock_pod
            result = operator.get_job_status("mynamespace:spark-abc-driver", {})

        assert result == "Pending"

    def test_k8s_get_job_status_returns_not_found_on_404(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        operator._hook = self._make_k8s_hook()

        class FakeApiException(Exception):
            def __init__(self, status):
                self.status = status

        with mock.patch("airflow.providers.apache.spark.operators.spark_submit.kube_client") as mock_kube:
            mock_kube.ApiException = FakeApiException
            mock_kube.get_kube_client.return_value.read_namespaced_pod.side_effect = FakeApiException(404)
            result = operator.get_job_status("mynamespace:spark-abc-driver", {})

        assert result == "NotFound"

    def test_k8s_get_job_status_reraises_non_404_api_exception(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        operator._hook = self._make_k8s_hook()

        class FakeApiException(Exception):
            def __init__(self, status):
                self.status = status

        with mock.patch("airflow.providers.apache.spark.operators.spark_submit.kube_client") as mock_kube:
            mock_kube.ApiException = FakeApiException
            mock_kube.get_kube_client.return_value.read_namespaced_pod.side_effect = FakeApiException(500)
            with pytest.raises(FakeApiException):
                operator.get_job_status("mynamespace:spark-abc-driver", {})

    def test_k8s_poll_until_complete_sets_pod_name_and_calls_poll_api(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        hook = self._make_k8s_hook()
        operator._hook = hook

        operator.poll_until_complete("mynamespace:spark-abc-driver", {})

        assert hook._kubernetes_driver_pod == "spark-abc-driver"
        hook._poll_k8s_driver_via_api.assert_called_once()

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="task_state_store requires Airflow 3.3+")
    def test_k8s_poll_until_complete_writes_succeeded_to_task_store(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        hook = self._make_k8s_hook()
        hook._poll_k8s_driver_via_api.return_value = "Succeeded"
        operator._hook = hook
        task_store = FakeTaskStateStore()

        operator.poll_until_complete("mynamespace:spark-abc-driver", {"task_state_store": task_store})

        assert task_store.get("k8s_driver_status") == "Succeeded"

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="task_state_store requires Airflow 3.3+")
    def test_k8s_polling_does_not_write_task_store_when_reconnect_disabled(self):
        operator = self._make_operator(track_driver_via_k8s_api=True, durable=False)
        hook = self._make_k8s_hook()
        hook._poll_k8s_driver_via_api.return_value = "Succeeded"
        operator._hook = hook
        task_store = FakeTaskStateStore()

        operator.poll_until_complete("mynamespace:spark-abc-driver", {"task_state_store": task_store})

        assert task_store.get("k8s_driver_status") is None

    def test_k8s_poll_until_complete_does_not_cache_and_reraises_on_failure(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        hook = self._make_k8s_hook()
        hook._poll_k8s_driver_via_api.side_effect = RuntimeError("Spark application failed (phase=Failed)")
        operator._hook = hook
        task_store = FakeTaskStateStore()

        with pytest.raises(RuntimeError, match="phase=Failed"):
            operator.poll_until_complete("mynamespace:spark-abc-driver", {"task_state_store": task_store})

        assert task_store.get("k8s_driver_status") is None

    def test_k8s_poll_until_complete_tolerates_absent_task_store(self):
        operator = self._make_operator(track_driver_via_k8s_api=True)
        operator._hook = self._make_k8s_hook()

        operator.poll_until_complete("mynamespace:spark-abc-driver", {})

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state, available in Airflow 3.3+",
    )
    def test_k8s_execute_persists_pod_id_when_durable(self):
        """execute() with durable=True stores the pod ID in task_store before polling."""
        operator = self._make_operator(track_driver_via_k8s_api=True, durable=True)
        hook = self._make_k8s_hook()
        hook._kubernetes_driver_pod = "spark-abc-driver"
        hook._connection = {"namespace": "mynamespace"}
        operator._hook = hook
        task_store = FakeTaskStateStore()
        persisted_before_poll: list[str | None] = []

        def track_poll(external_id, context):
            persisted_before_poll.append(task_store.get("spark_job_id"))

        operator.poll_until_complete = track_poll

        operator.execute(context={"task_state_store": task_store})

        assert persisted_before_poll == ["mynamespace:spark-abc-driver"]

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state, available in Airflow 3.3+",
    )
    def test_k8s_execute_durable_false_does_not_persist_pod_id(self):
        """execute() with durable=False does not write spark_job_id to task_store."""
        operator = self._make_operator(track_driver_via_k8s_api=True, durable=False)
        hook = self._make_k8s_hook()
        hook._kubernetes_driver_pod = "spark-abc-driver"
        hook._connection = {"namespace": "mynamespace"}
        operator._hook = hook
        task_store = FakeTaskStateStore()

        operator.poll_until_complete = lambda external_id, context: None

        operator.execute(context={"task_state_store": task_store})

        assert task_store.get("spark_job_id") is None
