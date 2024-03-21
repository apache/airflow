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

from datetime import timedelta

import pytest

from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone

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
        "queue": "yarn_dev_queue2",
        "deploy_mode": "client2",
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", default_args=args)

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
            "queue": "yarn_dev_queue2",
            "deploy_mode": "client2",
            "use_krb5ccache": True,
            "properties_file": "conf/spark-custom.conf",
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
        assert expected_dict["queue"] == operator._queue
        assert expected_dict["deploy_mode"] == operator._deploy_mode
        assert expected_dict["properties_file"] == operator.properties_file
        assert expected_dict["use_krb5ccache"] == operator._use_krb5ccache

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
        assert "--queue yarn_dev_queue2" in cmd
        assert "--deploy-mode client2" in cmd
        assert "sparky" in cmd

        # if we don't pass any overrides in arguments
        config["queue"] = None
        config["deploy_mode"] = None
        operator2 = SparkSubmitOperator(task_id="spark_submit_job2", dag=self.dag, **config)
        cmd2 = " ".join(operator2._get_hook()._build_spark_submit_command("test"))
        assert "--queue root.default" in cmd2
        assert "--deploy-mode client2" not in cmd2
        assert "spark-submit" in cmd2

    @pytest.mark.db_test
    def test_render_template(self):
        # Given
        operator = SparkSubmitOperator(task_id="spark_submit_job", dag=self.dag, **self._config)
        ti = TaskInstance(operator, run_id="spark_test")
        ti.dag_run = DagRun(dag_id=self.dag.dag_id, run_id="spark_test", execution_date=DEFAULT_DATE)

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
    def test_templating_with_create_task_instance_of_operator(self, create_task_instance_of_operator):
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
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
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
