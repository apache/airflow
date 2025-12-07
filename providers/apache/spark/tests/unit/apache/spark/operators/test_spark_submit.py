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
from datetime import timedelta
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

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
            ti = TaskInstance(operator, run_id="spark_test", dag_version_id=dag_version.id)
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
        ti.render_templates()

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
        ti.render_templates()
        task: SparkSubmitOperator = ti.task
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
