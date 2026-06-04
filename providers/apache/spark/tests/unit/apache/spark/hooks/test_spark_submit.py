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

import base64
import os
from io import StringIO
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, call, mock_open, patch

import kubernetes
import pytest
import requests
from kubernetes.client import V1Pod, V1PodStatus

from airflow.models import Connection
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
from airflow.providers.cncf.kubernetes import kube_client
from airflow.providers.common.compat.sdk import AirflowException


class TestSparkSubmitHook:
    _spark_job_file = "test_application.py"
    _config = {
        "conf": {"parquet.compression": "SNAPPY"},
        "conn_id": "default_spark",
        "files": "hive-site.xml",
        "py_files": "sample_library.py",
        "archives": "sample_archive.zip#SAMPLE",
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
        "name": "spark-job",
        "num_executors": 10,
        "verbose": True,
        "driver_memory": "3g",
        "java_class": "com.foo.bar.AppMain",
        "application_args": [
            "-f",
            "foo",
            "--bar",
            "bar",
            "--with-spaces",
            "args should keep embedded spaces",
            "baz",
        ],
        "use_krb5ccache": True,
    }

    @staticmethod
    def cmd_args_to_dict(list_cmd):
        return_dict = {}
        for arg1, arg2 in zip(list_cmd, list_cmd[1:]):
            if arg1.startswith("--"):
                return_dict[arg1] = arg2
        return return_dict

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="spark_yarn_cluster",
                conn_type="spark",
                host="yarn://yarn-master",
                extra='{"queue": "root.etl", "deploy-mode": "cluster"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_k8s_cluster",
                conn_type="spark",
                host="k8s://https://k8s-master",
                extra='{"deploy-mode": "cluster", "namespace": "mynamespace"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_k8s_client",
                conn_type="spark",
                host="k8s://https://k8s-master",
                extra='{"deploy-mode": "client", "namespace": "mynamespace"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_k8s_cluster_no_namespace",
                conn_type="spark",
                host="k8s://https://k8s-master",
                extra='{"deploy-mode": "cluster"}',
            )
        )
        create_connection_without_db(
            Connection(conn_id="spark_default_mesos", conn_type="spark", host="mesos://host", port=5050)
        )

        create_connection_without_db(
            Connection(
                conn_id="spark_binary_set",
                conn_type="spark",
                host="yarn",
                extra='{"spark-binary": "spark2-submit"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_binary_set_spark3_submit",
                conn_type="spark",
                host="yarn",
                extra='{"spark-binary": "spark3-submit"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_custom_binary_set",
                conn_type="spark",
                host="yarn",
                extra='{"spark-binary": "spark-other-submit"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_home_set",
                conn_type="spark",
                host="yarn",
                extra='{"spark-home": "/custom/spark-home/path"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_standalone_cluster",
                conn_type="spark",
                host="spark://spark-standalone-master:6066",
                extra='{"deploy-mode": "cluster"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_standalone_cluster_client_mode",
                conn_type="spark",
                host="spark://spark-standalone-master:6066",
                extra='{"deploy-mode": "client"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_principal_set",
                conn_type="spark",
                host="yarn",
                extra='{"principal": "user/spark@airflow.org"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_keytab_set",
                conn_type="spark",
                host="yarn",
                extra='{"keytab": "privileged_user.keytab"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_uri_with_protocol",
                uri="spark://spark-master:7077",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_uri_yarn",
                uri="yarn://yarn-master",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="mesos_uri",
                uri="mesos://mesos-host:5050",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="k8s_uri",
                uri="k8s://https://k8s-host:443",
            )
        )

        create_connection_without_db(
            Connection(
                conn_id="local_uri",
                uri="spark://local",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_yarn_rm",
                conn_type="spark",
                host="yarn",
                extra=(
                    '{"deploy-mode": "cluster", "yarn_resourcemanager_webapp_address": "http://rm.test:8088"}'
                ),
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="spark_yarn_rm_kerberos",
                conn_type="spark",
                host="yarn",
                extra=(
                    '{"deploy-mode": "cluster", '
                    '"yarn_resourcemanager_webapp_address": "http://rm.test:8088", '
                    '"principal": "airflow@EXAMPLE.COM", '
                    '"keytab": "cHJpdmlsZWdlZF91c2VyLmtleXRhYg=="}'
                ),
            )
        )

    @pytest.mark.db_test
    @patch(
        "airflow.providers.apache.spark.hooks.spark_submit.os.getenv", return_value="/tmp/airflow_krb5_ccache"
    )
    def test_build_spark_submit_command(self, mock_get_env, sdk_connection_not_found):
        # Given
        hook = SparkSubmitHook(**self._config)

        # When
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_build_cmd = [
            "spark-submit",
            "--master",
            "yarn",
            "--conf",
            "parquet.compression=SNAPPY",
            "--files",
            "hive-site.xml",
            "--py-files",
            "sample_library.py",
            "--archives",
            "sample_archive.zip#SAMPLE",
            "--jars",
            "parquet.jar",
            "--packages",
            "com.databricks:spark-avro_2.11:3.2.0",
            "--exclude-packages",
            "org.bad.dependency:1.0.0",
            "--repositories",
            "http://myrepo.org",
            "--num-executors",
            "10",
            "--total-executor-cores",
            "4",
            "--executor-cores",
            "4",
            "--executor-memory",
            "22g",
            "--driver-memory",
            "3g",
            "--keytab",
            "privileged_user.keytab",
            "--principal",
            "user/spark@airflow.org",
            "--conf",
            "spark.kerberos.renewal.credentials=ccache",
            "--proxy-user",
            "sample_user",
            "--name",
            "spark-job",
            "--class",
            "com.foo.bar.AppMain",
            "--verbose",
            "test_application.py",
            "-f",
            "foo",
            "--bar",
            "bar",
            "--with-spaces",
            "args should keep embedded spaces",
            "baz",
        ]
        assert expected_build_cmd == cmd
        mock_get_env.assert_called_with("KRB5CCNAME")

    @patch("airflow.configuration.conf.get_mandatory_value")
    def test_resolve_spark_submit_env_vars_use_krb5ccache_missing_principal(self, mock_get_madantory_value):
        mock_principal = "airflow"
        mock_get_madantory_value.return_value = mock_principal
        hook = SparkSubmitHook(conn_id="spark_yarn_cluster", principal=None, use_krb5ccache=True)
        mock_get_madantory_value.assert_called_with("kerberos", "principal")
        assert hook._principal == mock_principal

    def test_resolve_spark_submit_env_vars_use_krb5ccache_missing_KRB5CCNAME_env(self):
        hook = SparkSubmitHook(
            conn_id="spark_yarn_cluster", principal="user/spark@airflow.org", use_krb5ccache=True
        )
        with pytest.raises(
            AirflowException,
            match="KRB5CCNAME environment variable required to use ticket ccache is missing.",
        ):
            hook._build_spark_submit_command(self._spark_job_file)

    def test_build_track_driver_status_command(self):
        # note this function is only relevant for spark setup matching below condition
        # 'spark://' in self._connection['master'] and self._connection['deploy_mode'] == 'cluster'

        # Given
        hook_spark_standalone_cluster = SparkSubmitHook(conn_id="spark_standalone_cluster")
        hook_spark_standalone_cluster._driver_id = "driver-20171128111416-0001"
        hook_spark_yarn_cluster = SparkSubmitHook(conn_id="spark_yarn_cluster")
        hook_spark_yarn_cluster._driver_id = "driver-20171128111417-0001"

        # When
        build_track_driver_status_spark_standalone_cluster = (
            hook_spark_standalone_cluster._build_track_driver_status_command()
        )
        build_track_driver_status_spark_yarn_cluster = (
            hook_spark_yarn_cluster._build_track_driver_status_command()
        )

        # Then
        expected_spark_standalone_cluster = [
            "/usr/bin/curl",
            "--max-time",
            "30",
            "http://spark-standalone-master:6066/v1/submissions/status/driver-20171128111416-0001",
        ]
        expected_spark_yarn_cluster = [
            "spark-submit",
            "--master",
            "yarn://yarn-master",
            "--status",
            "driver-20171128111417-0001",
        ]

        assert expected_spark_standalone_cluster == build_track_driver_status_spark_standalone_cluster
        assert expected_spark_yarn_cluster == build_track_driver_status_spark_yarn_cluster

    @pytest.mark.db_test
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_spark_process_runcmd(self, mock_popen, sdk_connection_not_found):
        # Given
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.wait.return_value = 0

        # When

        hook = SparkSubmitHook(conn_id="")
        hook.submit()

        # Then
        assert mock_popen.mock_calls[0] == call(
            ["spark-submit", "--master", "yarn", "--name", "default-name", ""],
            stderr=-2,
            stdout=-1,
            universal_newlines=True,
            bufsize=-1,
        )

    @pytest.mark.db_test
    def test_resolve_should_track_driver_status(self, sdk_connection_not_found):
        # Given
        hook_default = SparkSubmitHook(conn_id="")
        hook_spark_yarn_cluster = SparkSubmitHook(conn_id="spark_yarn_cluster")
        hook_spark_k8s_cluster = SparkSubmitHook(conn_id="spark_k8s_cluster")
        hook_spark_default_mesos = SparkSubmitHook(conn_id="spark_default_mesos")
        hook_spark_binary_set = SparkSubmitHook(conn_id="spark_binary_set")
        hook_spark_standalone_cluster = SparkSubmitHook(conn_id="spark_standalone_cluster")

        # When
        should_track_driver_status_default = hook_default._resolve_should_track_driver_status()
        should_track_driver_status_spark_yarn_cluster = (
            hook_spark_yarn_cluster._resolve_should_track_driver_status()
        )
        should_track_driver_status_spark_k8s_cluster = (
            hook_spark_k8s_cluster._resolve_should_track_driver_status()
        )
        should_track_driver_status_spark_default_mesos = (
            hook_spark_default_mesos._resolve_should_track_driver_status()
        )
        should_track_driver_status_spark_binary_set = (
            hook_spark_binary_set._resolve_should_track_driver_status()
        )
        should_track_driver_status_spark_standalone_cluster = (
            hook_spark_standalone_cluster._resolve_should_track_driver_status()
        )

        # Then
        assert should_track_driver_status_default is False
        assert should_track_driver_status_spark_yarn_cluster is False
        assert should_track_driver_status_spark_k8s_cluster is False
        assert should_track_driver_status_spark_default_mesos is False
        assert should_track_driver_status_spark_binary_set is False
        assert should_track_driver_status_spark_standalone_cluster is True

    @pytest.mark.db_test
    def test_resolve_connection_yarn_default(self, sdk_connection_not_found):
        # Given
        hook = SparkSubmitHook(conn_id="")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark-submit",
            "deploy_mode": None,
            "queue": None,
            "namespace": None,
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--master"] == "yarn"

    @pytest.mark.db_test
    def test_resolve_connection_yarn_default_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_default")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark-submit",
            "deploy_mode": None,
            "queue": "root.default",
            "namespace": None,
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--master"] == "yarn"
        assert dict_cmd["--queue"] == "root.default"

    def test_resolve_connection_mesos_default_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_default_mesos")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "master": "mesos://host:5050",
            "spark_binary": "spark-submit",
            "deploy_mode": None,
            "queue": None,
            "namespace": None,
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--master"] == "mesos://host:5050"

    def test_resolve_connection_spark_yarn_cluster_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_yarn_cluster")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "master": "yarn://yarn-master",
            "spark_binary": "spark-submit",
            "deploy_mode": "cluster",
            "queue": "root.etl",
            "namespace": None,
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--master"] == "yarn://yarn-master"
        assert dict_cmd["--queue"] == "root.etl"
        assert dict_cmd["--deploy-mode"] == "cluster"

    def test_resolve_connection_spark_k8s_cluster_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "queue": None,
            "spark_binary": "spark-submit",
            "master": "k8s://https://k8s-master",
            "deploy_mode": "cluster",
            "namespace": "mynamespace",
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--master"] == "k8s://https://k8s-master"
        assert dict_cmd["--deploy-mode"] == "cluster"

    def test_resolve_connection_spark_k8s_cluster_ns_conf(self):
        # Given we specify the config option directly
        conf = {
            "spark.kubernetes.namespace": "airflow",
        }
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", conf=conf)

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "queue": None,
            "spark_binary": "spark-submit",
            "master": "k8s://https://k8s-master",
            "deploy_mode": "cluster",
            "namespace": "airflow",
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--master"] == "k8s://https://k8s-master"
        assert dict_cmd["--deploy-mode"] == "cluster"
        assert dict_cmd["--conf"] == "spark.kubernetes.namespace=airflow"

    def test_resolve_connection_spark_binary_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_binary_set")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark2-submit",
            "deploy_mode": None,
            "queue": None,
            "namespace": None,
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert cmd[0] == "spark2-submit"

    def test_resolve_connection_spark_binary_spark3_submit_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_binary_set_spark3_submit")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark3-submit",
            "deploy_mode": None,
            "queue": None,
            "namespace": None,
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert cmd[0] == "spark3-submit"

    def test_resolve_connection_spark_uri_with_protocol(self):
        hook = SparkSubmitHook(conn_id="spark_uri_with_protocol")
        connection = hook._resolve_connection()
        assert connection["master"] == "spark://spark-master:7077"

    def test_resolve_connection_spark_uri_yarn(self):
        hook = SparkSubmitHook(conn_id="spark_uri_yarn")
        connection = hook._resolve_connection()
        assert connection["master"] == "yarn://yarn-master"

    def test_resolve_connection_mesos_uri(self):
        hook = SparkSubmitHook(conn_id="mesos_uri")
        connection = hook._resolve_connection()
        assert connection["master"] == "mesos://mesos-host:5050"

    def test_resolve_connection_k8s_uri(self):
        hook = SparkSubmitHook(conn_id="k8s_uri")
        connection = hook._resolve_connection()
        assert connection["master"] == "k8s://https://k8s-host:443"

    def test_resolve_connection_local_uri(self):
        hook = SparkSubmitHook(conn_id="local_uri")
        connection = hook._resolve_connection()
        assert connection["master"] == "local"

    def test_resolve_connection_custom_spark_binary_allowed_in_hook(self):
        SparkSubmitHook(conn_id="spark_binary_set", spark_binary="another-custom-spark-submit")

    def test_resolve_connection_spark_binary_extra_not_allowed_runtime_error(self):
        with pytest.raises(
            ValueError,
            match="Please make sure your spark binary is one of the allowed ones and that it is available on the PATH",
        ):
            SparkSubmitHook(conn_id="spark_custom_binary_set")

    def test_resolve_connection_spark_home_not_allowed_runtime_error(self):
        with pytest.raises(ValueError, match="The `spark-home` extra is not allowed any more"):
            SparkSubmitHook(conn_id="spark_home_set")

    def test_resolve_connection_spark_binary_default_value_override(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_binary_set", spark_binary="spark3-submit")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark3-submit",
            "deploy_mode": None,
            "queue": None,
            "namespace": None,
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert cmd[0] == "spark3-submit"

    @pytest.mark.db_test
    def test_resolve_connection_spark_binary_default_value(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_default")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark-submit",
            "deploy_mode": None,
            "queue": "root.default",
            "namespace": None,
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert cmd[0] == "spark-submit"

    def test_resolve_connection_spark_standalone_cluster_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_standalone_cluster")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {
            "master": "spark://spark-standalone-master:6066",
            "spark_binary": "spark-submit",
            "deploy_mode": "cluster",
            "queue": None,
            "namespace": None,
            "principal": None,
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert cmd[0] == "spark-submit"

    def test_resolve_connection_principal_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_principal_set")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark-submit",
            "deploy_mode": None,
            "queue": None,
            "namespace": None,
            "principal": "user/spark@airflow.org",
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--principal"] == "user/spark@airflow.org"

    def test_resolve_connection_principal_value_override(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_principal_set", principal="will-override")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark-submit",
            "deploy_mode": None,
            "queue": None,
            "namespace": None,
            "principal": "will-override",
            "keytab": None,
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--principal"] == "will-override"

    @patch(
        "airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook._create_keytab_path_from_base64_keytab",
        return_value="privileged_user.keytab",
    )
    def test_resolve_connection_keytab_set_connection(self, mock_create_keytab_path_from_base64_keytab):
        # Given
        hook = SparkSubmitHook(conn_id="spark_keytab_set")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark-submit",
            "deploy_mode": None,
            "queue": None,
            "namespace": None,
            "principal": None,
            "keytab": "privileged_user.keytab",
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--keytab"] == "privileged_user.keytab"

    @patch(
        "airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook._create_keytab_path_from_base64_keytab"
    )
    def test_resolve_connection_keytab_value_override(self, mock_create_keytab_path_from_base64_keytab):
        # Given
        hook = SparkSubmitHook(conn_id="spark_keytab_set", keytab="will-override")

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {
            "master": "yarn",
            "spark_binary": "spark-submit",
            "deploy_mode": None,
            "queue": None,
            "namespace": None,
            "principal": None,
            "keytab": "will-override",
            "rest_scheme": "http",
            "rest_port": 6066,
        }
        assert connection == expected_spark_connection
        assert dict_cmd["--keytab"] == "will-override"
        assert not mock_create_keytab_path_from_base64_keytab.called, (
            "Should not call _create_keytab_path_from_base64_keytab"
        )

    def test_resolve_spark_submit_env_vars_standalone_client_mode(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_standalone_cluster_client_mode", env_vars={"bar": "foo"})

        # When
        hook._build_spark_submit_command(self._spark_job_file)

        # Then
        assert hook._env == {"bar": "foo"}

    def test_resolve_spark_submit_env_vars_standalone_cluster_mode(self):
        def env_vars_exception_in_standalone_cluster_mode():
            # Given
            hook = SparkSubmitHook(conn_id="spark_standalone_cluster", env_vars={"bar": "foo"})

            # When
            hook._build_spark_submit_command(self._spark_job_file)

        # Then
        with pytest.raises(AirflowException):
            env_vars_exception_in_standalone_cluster_mode()

    def test_resolve_spark_submit_env_vars_yarn(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_yarn_cluster", env_vars={"bar": "foo"})

        # When
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        assert cmd[4] == "spark.yarn.appMasterEnv.bar=foo"
        assert hook._env == {"bar": "foo"}

    def test_resolve_spark_submit_env_vars_k8s(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", env_vars={"bar": "foo"})

        # When
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        assert cmd[4] == "spark.kubernetes.driverEnv.bar=foo"

    def test_process_spark_submit_log_yarn(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_yarn_cluster")
        log_lines = [
            "SPARK_MAJOR_VERSION is set to 2, using Spark2",
            "WARN NativeCodeLoader: Unable to load native-hadoop library for your "
            "platform... using builtin-java classes where applicable",
            "WARN DomainSocketFactory: The short-circuit local reads feature cannot "
            "be used because libhadoop cannot be loaded.",
            "INFO Client: Requesting a new application from cluster with 10 NodeManagers",
            "INFO Client: Submitting application application_1486558679801_1820 to ResourceManager",
        ]
        # When
        hook._process_spark_submit_log(log_lines)

        # Then

        assert hook._yarn_application_id == "application_1486558679801_1820"

    @pytest.mark.parametrize(
        "pod_name",
        [
            "spark-pi-edf2ace37be7353a958b38733a12f8e6-driver",
            "spark-pi-driver-edf2ace37be7353a958b38733a12f8e6-driver",
        ],
    )
    def test_process_spark_submit_log_k8s(self, pod_name):
        # Given
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster")
        log_lines = [
            "INFO  LoggingPodStatusWatcherImpl:54 - State changed, new state:",
            f"pod name: {pod_name}",
            "namespace: default",
            "labels: spark-app-selector -> spark-465b868ada474bda82ccb84ab2747fcd, spark-role -> driver",
            "pod uid: ba9c61f6-205f-11e8-b65f-d48564c88e42",
            "creation time: 2018-03-05T10:26:55Z",
            "service account name: spark",
            "volumes: spark-init-properties, download-jars-volume,download-files-volume, spark-token-2vmlm",
            "node name: N/A",
            "start time: N/A",
            "container images: N/A",
            "phase: Pending",
            "status: []",
            "2018-03-05 11:26:56 INFO  LoggingPodStatusWatcherImpl:54 - State changed, new state:",
            f"pod name: {pod_name}",
            "namespace: default",
            "Exit code: 999",
        ]

        # When
        hook._process_spark_submit_log(log_lines)

        # Then
        assert hook._kubernetes_driver_pod == pod_name
        assert hook._kubernetes_application_id == "spark-465b868ada474bda82ccb84ab2747fcd"
        assert hook._spark_exit_code == 999

    def test_process_spark_submit_log_k8s_spark_3(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster")
        log_lines = ["exit code: 999"]

        # When
        hook._process_spark_submit_log(log_lines)

        # Then
        assert hook._spark_exit_code == 999

    def test_process_spark_submit_log_k8s_submission_id_format(self):
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster")
        log_lines = [
            "INFO Client: Deployed Spark application arrow-spark with application ID "
            "spark-1e22d65826b74ac2927249b0e607ed54 and submission ID "
            "spark:arrow-spark-c8e2e29e73db9c93-driver into Kubernetes",
        ]

        hook._process_spark_submit_log(log_lines)

        assert hook._kubernetes_driver_pod == "arrow-spark-c8e2e29e73db9c93-driver"

    def test_process_spark_client_mode_submit_log_k8s(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_k8s_client")
        log_lines = [
            "INFO - The executor with id 2 exited with exit code 137(SIGKILL, possible container OOM).",
            "...",
            "Pi is roughly 3.141640",
            "SparkContext: Successfully stopped SparkContext",
        ]

        # When
        hook._process_spark_submit_log(log_lines)

        # Then
        assert hook._spark_exit_code == 0

    def test_process_spark_submit_log_standalone_cluster(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_standalone_cluster")
        log_lines = [
            "Running Spark using the REST application submission protocol.",
            "17/11/28 11:14:15 INFO RestSubmissionClient: Submitting a request "
            "to launch an application in spark://spark-standalone-master:6066",
            "17/11/28 11:14:15 INFO RestSubmissionClient: Submission successfully "
            "created as driver-20171128111415-0001. Polling submission state...",
        ]
        # When
        hook._process_spark_submit_log(log_lines)

        # Then

        assert hook._driver_id == "driver-20171128111415-0001"

    def test_process_spark_driver_status_log(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_standalone_cluster")
        log_lines = [
            "Submitting a request for the status of submission "
            "driver-20171128111415-0001 in spark://spark-standalone-master:6066",
            "17/11/28 11:15:37 INFO RestSubmissionClient: Server responded with SubmissionStatusResponse:",
            "{",
            '"action" : "SubmissionStatusResponse",',
            '"driverState" : "RUNNING",',
            '"serverSparkVersion" : "1.6.0",',
            '"submissionId" : "driver-20171128111415-0001",',
            '"success" : true,',
            '"workerHostPort" : "172.18.0.7:38561",',
            '"workerId" : "worker-20171128110741-172.18.0.7-38561"',
            "}",
        ]
        # When
        hook._process_spark_status_log(log_lines)

        # Then

        assert hook._driver_status == "RUNNING"

    def test_process_spark_driver_status_log_bad_response(self):
        # Given
        hook = SparkSubmitHook(conn_id="spark_standalone_cluster")
        log_lines = [
            "curl: Failed to connect to http://spark-standalone-master:6066This is an invalid Spark response",
            "Timed out",
        ]
        # When
        hook._process_spark_status_log(log_lines)

        # Then

        assert hook._driver_status is None

    @patch("airflow.providers.apache.spark.hooks.spark_submit.renew_from_kt")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_process_on_kill(self, mock_popen, mock_renew_from_kt):
        # Given
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0
        log_lines = [
            "SPARK_MAJOR_VERSION is set to 2, using Spark2",
            "WARN NativeCodeLoader: Unable to load native-hadoop library for your "
            "platform... using builtin-java classes where applicable",
            "WARN DomainSocketFactory: The short-circuit local reads feature cannot "
            "be used because libhadoop cannot be loaded.",
            "INFO Client: Requesting a new application from cluster with 10 "
            "NodeManagerapplication_1486558679801_1820s",
            "INFO Client: Submitting application application_1486558679801_1820 to ResourceManager",
        ]
        env = {"PATH": "hadoop/bin"}
        hook = SparkSubmitHook(conn_id="spark_yarn_cluster", env_vars=env)
        hook._process_spark_submit_log(log_lines)
        hook.submit()

        # When
        hook.on_kill()

        # Then
        assert (
            call(
                ["yarn", "application", "-kill", "application_1486558679801_1820"],
                env={**os.environ, **env},
                stderr=-1,
                stdout=-1,
            )
            in mock_popen.mock_calls
        )
        # resetting the mock to test  kill with keytab & principal
        mock_popen.reset_mock()
        # Given
        hook = SparkSubmitHook(
            conn_id="spark_yarn_cluster", keytab="privileged_user.keytab", principal="user/spark@airflow.org"
        )
        hook._process_spark_submit_log(log_lines)
        hook.submit()

        # When
        hook.on_kill()
        # Then
        expected_env = os.environ.copy()
        expected_env["KRB5CCNAME"] = "/tmp/airflow_krb5_ccache"
        assert (
            call(
                ["yarn", "application", "-kill", "application_1486558679801_1820"],
                env=expected_env,
                stderr=-1,
                stdout=-1,
            )
            in mock_popen.mock_calls
        )

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_legacy_on_kill_skips_yarn_cli_when_submit_sp_already_exited(self, mock_popen):
        """Regression guard when `yarn_track_via_rm_api=False` (legacy
        path), the YARN CLI kill must stay gated on a live `_submit_sp`. If the
        spark-submit subprocess has already exited, `on_kill` must not spawn
        `yarn application -kill` — preserving pre-PR behavior for users who have
        not opted in to the REST kill path.
        """
        submit_process = MagicMock(spec=["kill", "poll"])
        submit_process.poll.return_value = 0  # already exited

        hook = SparkSubmitHook(conn_id="spark_yarn_cluster")  # yarn_track_via_rm_api=False
        hook._submit_sp = submit_process
        hook._yarn_application_id = "application_1486558679801_1820"

        hook.on_kill()

        submit_process.kill.assert_not_called()
        mock_popen.assert_not_called()

    def test_standalone_cluster_process_on_kill(self):
        # Given
        log_lines = [
            "Running Spark using the REST application submission protocol.",
            "17/11/28 11:14:15 INFO RestSubmissionClient: Submitting a request "
            "to launch an application in spark://spark-standalone-master:6066",
            "17/11/28 11:14:15 INFO RestSubmissionClient: Submission successfully "
            "created as driver-20171128111415-0001. Polling submission state...",
        ]
        hook = SparkSubmitHook(conn_id="spark_standalone_cluster")
        hook._process_spark_submit_log(log_lines)

        # When
        kill_cmd = hook._build_spark_driver_kill_command()

        # Then
        assert kill_cmd[0] == "spark-submit"
        assert kill_cmd[1] == "--master"
        assert kill_cmd[2] == "spark://spark-standalone-master:6066"
        assert kill_cmd[3] == "--kill"
        assert kill_cmd[4] == "driver-20171128111415-0001"

    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_k8s_process_on_kill(self, mock_popen, mock_client_method):
        # Given
        mock_popen.return_value.stdout = StringIO("stdout")
        mock_popen.return_value.stderr = StringIO("stderr")
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0
        client = mock_client_method.return_value
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster")
        log_lines = [
            "INFO  LoggingPodStatusWatcherImpl:54 - State changed, new state:",
            "pod name: spark-pi-edf2ace37be7353a958b38733a12f8e6-driver",
            "namespace: default",
            "labels: spark-app-selector -> spark-465b868ada474bda82ccb84ab2747fcd, spark-role -> driver",
            "pod uid: ba9c61f6-205f-11e8-b65f-d48564c88e42",
            "creation time: 2018-03-05T10:26:55Z",
            "service account name: spark",
            "volumes: spark-init-properties, download-jars-volume,download-files-volume, spark-token-2vmlm",
            "node name: N/A",
            "start time: N/A",
            "container images: N/A",
            "phase: Pending",
            "status: []",
            "2018-03-05 11:26:56 INFO  LoggingPodStatusWatcherImpl:54 - State changed, new state:",
            "pod name: spark-pi-edf2ace37be7353a958b38733a12f8e6-driver",
            "namespace: default",
            "Exit code: 0",
        ]
        hook._process_spark_submit_log(log_lines)
        hook.submit()

        # When
        hook.on_kill()

        # Then
        import kubernetes

        kwargs = {"pretty": True, "body": kubernetes.client.V1DeleteOptions()}
        client.delete_namespaced_pod.assert_called_once_with(
            "spark-pi-edf2ace37be7353a958b38733a12f8e6-driver", "mynamespace", **kwargs
        )

    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_on_kill_deletes_pod_when_k8s_api_tracking_and_submit_sp_already_exited(self, mock_get_client):
        """on_kill must delete the driver pod when K8s-API tracking is active even if spark-submit
        has already exited.
        """
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", track_driver_via_k8s_api=True)
        hook._kubernetes_driver_pod = "spark-app-abc-driver"
        hook._kubernetes_application_id = "spark-abc"
        hook._submit_sp = MagicMock()
        # spark-submit already exited
        hook._submit_sp.poll.return_value = 0

        mock_client = mock_get_client.return_value

        hook.on_kill()

        mock_client.delete_namespaced_pod.assert_called_once_with(
            "spark-app-abc-driver",
            "mynamespace",
            body=kubernetes.client.V1DeleteOptions(),
            pretty=True,
        )

    @pytest.mark.parametrize(
        ("command", "expected"),
        [
            (
                ("spark-submit", "foo", "--bar", "baz", "--password='secret'", "--foo", "bar"),
                "spark-submit foo --bar baz --password='******' --foo bar",
            ),
            (
                ("spark-submit", "foo", "--bar", "baz", "--password='secret'"),
                "spark-submit foo --bar baz --password='******'",
            ),
            (
                ("spark-submit", "foo", "--bar", "baz", '--password="secret"'),
                'spark-submit foo --bar baz --password="******"',
            ),
            (
                ("spark-submit", "foo", "--bar", "baz", "--password=secret"),
                "spark-submit foo --bar baz --password=******",
            ),
            (
                ("spark-submit", "foo", "--bar", "baz", "--password 'secret'"),
                "spark-submit foo --bar baz --password '******'",
            ),
            (
                ("spark-submit", "foo", "--bar", "baz", "--password='sec\"ret'"),
                "spark-submit foo --bar baz --password='******'",
            ),
            (
                ("spark-submit", "foo", "--bar", "baz", '--password="sec\'ret"'),
                'spark-submit foo --bar baz --password="******"',
            ),
            (
                ("spark-submit",),
                "spark-submit",
            ),
        ],
    )
    @pytest.mark.db_test
    def test_masks_passwords(self, command: str, expected: str) -> None:
        # Given
        hook = SparkSubmitHook()

        # When
        command_masked = hook._mask_cmd(command)

        # Then
        assert command_masked == expected

    @pytest.mark.db_test
    def test_create_keytab_path_from_base64_keytab_with_decode_exception(self):
        hook = SparkSubmitHook()
        invalid_base64 = "invalid_base64"

        with pytest.raises(AirflowException, match="Failed to decode base64 keytab"):
            hook._create_keytab_path_from_base64_keytab(invalid_base64, None)

    @pytest.mark.db_test
    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open)
    def test_create_keytab_path_from_base64_keytab_with_write_exception(
        self,
        mock_open,
        mock_exists,
    ):
        # Given
        hook = SparkSubmitHook()

        keytab_value = b"abcd"
        base64_keytab = base64.b64encode(keytab_value).decode("UTF-8")
        _mock_open = mock_open()
        _mock_open.write.side_effect = Exception("Write failed")
        mock_exists.return_value = False

        # When
        with pytest.raises(AirflowException, match="Failed to save keytab"):
            hook._create_keytab_path_from_base64_keytab(base64_keytab, None)

        # Then
        assert mock_exists.call_count == 2  # called twice (before write, after write)

    @pytest.mark.db_test
    @patch("airflow.providers.apache.spark.hooks.spark_submit.shutil.move")
    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open)
    def test_create_keytab_path_from_base64_keytab_with_move_exception(
        self,
        mock_open,
        mock_exists,
        mock_move,
    ):
        # Given
        hook = SparkSubmitHook()

        keytab_value = b"abcd"
        base64_keytab = base64.b64encode(keytab_value).decode("UTF-8")
        mock_exists.return_value = False
        mock_move.side_effect = Exception("Move failed")

        # When
        with pytest.raises(AirflowException, match="Failed to save keytab"):
            hook._create_keytab_path_from_base64_keytab(base64_keytab, None)

        # Then
        mock_open().write.assert_called_once_with(keytab_value)
        mock_move.assert_called_once()
        assert mock_exists.call_count == 2  # called twice (before write, after write)

    @pytest.mark.db_test
    @patch("airflow.providers.apache.spark.hooks.spark_submit.uuid.uuid4")
    @patch("pathlib.Path.resolve")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.shutil.move")
    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open)
    def test_create_keytab_path_from_base64_keytab_with_new_keytab(
        self,
        mock_open,
        mock_exists,
        mock_move,
        mock_resolve,
        mock_uuid4,
    ):
        # Given
        hook = SparkSubmitHook()

        keytab_value = b"abcd"
        base64_keytab = base64.b64encode(keytab_value).decode("UTF-8")
        mock_uuid4.return_value = "uuid"
        mock_resolve.return_value = Path("resolved_path")
        mock_exists.return_value = False

        # When
        keytab = hook._create_keytab_path_from_base64_keytab(base64_keytab, None)

        # Then
        assert keytab == "resolved_path/airflow_keytab-uuid"
        mock_open().write.assert_called_once_with(keytab_value)
        mock_move.assert_called_once()

    @pytest.mark.db_test
    @patch("pathlib.Path.resolve")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.shutil.move")
    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open)
    def test_create_keytab_path_from_base64_keytab_with_new_keytab_with_principal(
        self,
        mock_open,
        mock_exists,
        mock_move,
        mock_resolve,
    ):
        # Given
        hook = SparkSubmitHook()

        principal = "user/spark@airflow.org"
        keytab_value = b"abcd"
        base64_keytab = base64.b64encode(keytab_value).decode("UTF-8")
        mock_resolve.return_value = Path("resolved_path")
        mock_exists.return_value = False

        # When
        keytab = hook._create_keytab_path_from_base64_keytab(base64_keytab, principal)

        # Then
        assert keytab == f"resolved_path/airflow_keytab-{principal}"
        mock_open().write.assert_called_once_with(keytab_value)
        mock_move.assert_called_once()

    @pytest.mark.db_test
    @patch("pathlib.Path.resolve")
    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open)
    def test_create_keytab_path_from_base64_keytab_with_existing_keytab(
        self,
        mock_open,
        mock_exists,
        mock_resolve,
    ):
        # Given
        hook = SparkSubmitHook()

        principal = "user/spark@airflow.org"
        keytab_value = b"abcd"
        base64_keytab = base64.b64encode(keytab_value)
        mock_resolve.return_value = Path("resolved_path")
        mock_exists.return_value = True
        _mock_open = mock_open()
        _mock_open.read.return_value = keytab_value

        # When
        keytab = hook._create_keytab_path_from_base64_keytab(base64_keytab.decode("UTF-8"), principal)

        # Then
        assert keytab == f"resolved_path/airflow_keytab-{principal}"
        mock_open.assert_called_with(Path(f"resolved_path/airflow_keytab-{principal}"), "rb")
        _mock_open.read.assert_called_once()
        assert not _mock_open.write.called, "Keytab file should not be written"

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_run_post_submit_commands_success(self, mock_run):
        """Test that post_submit_commands are run with shell=False and shlex.split."""
        import subprocess

        mock_result = MagicMock(spec=subprocess.CompletedProcess)
        mock_result.returncode = 0
        mock_result.stdout = "sidecar terminated"
        mock_run.return_value = mock_result

        hook = SparkSubmitHook(
            conn_id="",
            post_submit_commands=["curl -X POST localhost:15020/quitquitquit"],
        )
        hook._run_post_submit_commands()

        mock_run.assert_called_once()
        call_args = mock_run.call_args
        assert (
            call_args.kwargs.get("shell") is False
            or call_args.args[1:2] == (False,)
            or not call_args.kwargs.get("shell", True)
        )
        # Verify shlex.split was applied — cmd was split into a list
        assert isinstance(call_args.args[0], list)
        assert call_args.args[0] == ["curl", "-X", "POST", "localhost:15020/quitquitquit"]

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_run_post_submit_commands_nonzero_exit_warns(self, mock_run):
        """Test that a non-zero exit code logs a warning but does not raise."""
        import subprocess

        mock_result = MagicMock(spec=subprocess.CompletedProcess)
        mock_result.returncode = 1
        mock_result.stdout = "error output"
        mock_run.return_value = mock_result

        hook = SparkSubmitHook(conn_id="", post_submit_commands=["false"])
        # Should not raise
        hook._run_post_submit_commands()
        mock_run.assert_called_once()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_run_post_submit_commands_timeout_warns(self, mock_run):
        """Test that a timeout logs a warning but does not raise."""
        import subprocess

        mock_run.side_effect = subprocess.TimeoutExpired(cmd="sleep 100", timeout=30)

        hook = SparkSubmitHook(conn_id="", post_submit_commands=["sleep 100"])
        # Should not raise
        hook._run_post_submit_commands()

    def test_post_submit_commands_list_is_copied(self):
        """Test that the commands list is copied, not stored by reference."""
        original = ["cmd1", "cmd2"]
        hook = SparkSubmitHook(conn_id="", post_submit_commands=original)
        original.append("cmd3")
        assert hook._post_submit_commands == ["cmd1", "cmd2"], (
            "Hook should not be affected by mutations to the original list"
        )

    def test_post_submit_commands_none_gives_empty_list(self):
        """Test that None post_submit_commands results in an empty list."""
        hook = SparkSubmitHook(conn_id="")
        assert hook._post_submit_commands == []

    @pytest.mark.parametrize(
        ("conn_id", "flag", "expected"),
        [
            ("spark_k8s_cluster", False, False),
            ("spark_k8s_cluster", True, True),
            ("spark_k8s_client", True, False),
        ],
    )
    def test_should_track_driver_via_k8s_api(self, conn_id, flag, expected):
        hook = SparkSubmitHook(conn_id=conn_id, track_driver_via_k8s_api=flag)
        assert hook._should_track_driver_via_k8s_api() is expected

    @pytest.mark.parametrize(
        ("conn_id", "match"),
        [
            ("spark_yarn_cluster", "requires Spark master to be Kubernetes"),
            ("spark_k8s_client", "requires `deploy_mode='cluster'`"),
            ("spark_k8s_cluster_no_namespace", "requires a namespace"),
        ],
    )
    def test_validate_track_driver_via_k8s_api_raises(self, conn_id, match):
        hook = SparkSubmitHook(conn_id=conn_id, track_driver_via_k8s_api=True)
        with pytest.raises(ValueError, match=match):
            hook._validate_track_driver_via_k8s_api_config()

    def test_validate_track_driver_via_k8s_api_raises_on_conflicting_user_conf(self):
        hook = SparkSubmitHook(
            conn_id="spark_k8s_cluster",
            track_driver_via_k8s_api=True,
            conf={"spark.kubernetes.submission.waitAppCompletion": "true"},
        )
        with pytest.raises(ValueError, match="incompatible with.*waitAppCompletion=true"):
            hook._validate_track_driver_via_k8s_api_config()

    def test_conf_injection_adds_wait_app_completion(self):
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", track_driver_via_k8s_api=True)
        cmd = hook._build_spark_submit_command("app.jar")
        conf_pairs = [cmd[i + 1] for i, v in enumerate(cmd) if v == "--conf"]
        assert "spark.kubernetes.submission.waitAppCompletion=false" in conf_pairs

    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_poll_k8s_driver_succeeds(self, mock_get_client):
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", track_driver_via_k8s_api=True)
        hook._kubernetes_driver_pod = "spark-app-abc-driver"
        hook._kubernetes_application_id = "spark-abc"

        mock_client = mock_get_client.return_value
        running_pod = V1Pod(status=V1PodStatus(phase="Running"))
        succeeded_pod = V1Pod(status=V1PodStatus(phase="Succeeded"))
        mock_client.read_namespaced_pod.side_effect = [running_pod, succeeded_pod]

        with patch.object(hook, "_run_post_submit_commands"):
            hook._poll_k8s_driver_via_api()

        assert mock_client.delete_namespaced_pod.call_args.args[:2] == ("spark-app-abc-driver", "mynamespace")

    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_poll_k8s_driver_raises_on_failed(self, mock_get_client):
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", track_driver_via_k8s_api=True)
        hook._kubernetes_driver_pod = "spark-app-abc-driver"
        hook._kubernetes_application_id = "spark-abc"

        mock_client = mock_get_client.return_value
        failed_pod = V1Pod(status=V1PodStatus(phase="Failed"))
        mock_client.read_namespaced_pod.return_value = failed_pod

        with pytest.raises(RuntimeError, match="phase=Failed"):
            hook._poll_k8s_driver_via_api()

    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_poll_k8s_driver_raises_after_consecutive_unknown(self, mock_get_client):
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", track_driver_via_k8s_api=True)
        hook._kubernetes_driver_pod = "spark-app-abc-driver"
        hook._kubernetes_application_id = "spark-abc"

        mock_client = mock_get_client.return_value
        mock_client.read_namespaced_pod.return_value = V1Pod(status=V1PodStatus(phase="Unknown"))

        with patch("time.sleep"), pytest.raises(RuntimeError, match="Unknown phase"):
            hook._poll_k8s_driver_via_api()

        # assert that it was polled minimum 3 times to confirm the Unknown status before raising
        assert mock_client.read_namespaced_pod.call_count == 3

    @patch("time.sleep")
    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_poll_k8s_driver_tolerates_transient_api_errors(self, mock_get_client, _):
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", track_driver_via_k8s_api=True)
        hook._kubernetes_driver_pod = "spark-app-abc-driver"
        hook._kubernetes_application_id = "spark-abc"

        mock_client = mock_get_client.return_value
        api_error = kube_client.ApiException(status=500, reason="Internal Server Error")
        succeeded_pod = V1Pod(status=V1PodStatus(phase="Succeeded"))
        mock_client.read_namespaced_pod.side_effect = [api_error, api_error, succeeded_pod]

        with patch.object(hook, "_run_post_submit_commands"):
            hook._poll_k8s_driver_via_api()

        assert mock_client.read_namespaced_pod.call_count == 3

    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_post_submit_commands_run_exactly_once_on_k8s_path(self, mock_get_client):
        """_run_post_submit_commands must fire exactly once: in _poll_k8s_driver_via_api finally."""
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", track_driver_via_k8s_api=True)
        hook._kubernetes_driver_pod = "spark-app-abc-driver"
        hook._kubernetes_application_id = "spark-abc"

        mock_client = mock_get_client.return_value
        mock_client.read_namespaced_pod.return_value = V1Pod(status=V1PodStatus(phase="Succeeded"))

        with patch.object(hook, "_run_post_submit_commands") as mock_cmd:
            hook._poll_k8s_driver_via_api()

        mock_cmd.assert_called_once()

    @patch("time.sleep")
    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_poll_k8s_driver_raises_after_consecutive_api_errors(self, mock_get_client, _):
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", track_driver_via_k8s_api=True)
        hook._kubernetes_driver_pod = "spark-app-abc-driver"
        hook._kubernetes_application_id = "spark-abc"

        mock_client = mock_get_client.return_value
        api_error = kube_client.ApiException(status=500, reason="Internal Server Error")
        mock_client.read_namespaced_pod.side_effect = api_error

        with pytest.raises(RuntimeError, match="K8s API unreachable"):
            hook._poll_k8s_driver_via_api()

        assert mock_client.read_namespaced_pod.call_count == 3

    @patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_poll_k8s_driver_exits_cleanly_on_404(self, mock_get_client):
        """404 from read_namespaced_pod means pod was deleted by on_kill — should return cleanly, not raise."""
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", track_driver_via_k8s_api=True)
        hook._kubernetes_driver_pod = "spark-app-abc-driver"
        hook._kubernetes_application_id = "spark-abc"

        mock_client = mock_get_client.return_value
        mock_client.read_namespaced_pod.side_effect = kube_client.ApiException(status=404, reason="Not Found")

        hook._poll_k8s_driver_via_api()

        mock_client.delete_namespaced_pod.assert_not_called()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_run_post_submit_commands_runs_only_once(self, mock_run):
        """Calling _run_post_submit_commands twice must execute commands exactly once."""
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", post_submit_commands=["echo done"])

        hook._run_post_submit_commands()
        hook._run_post_submit_commands()

        mock_run.assert_called_once()

    _YARN_LOG_LINES = [
        "INFO Client: Requesting a new application from cluster with 1 NodeManagers",
        "INFO Client: Uploading resource file:/tmp/lib.zip -> "
        "hdfs://namenode:8020/user/root/.sparkStaging/application_1700000000000_0001/lib.zip",
        "INFO Client: Submitting application application_1700000000000_0001 to ResourceManager",
        "INFO YarnClientImpl: Submitted application application_1700000000000_0001",
        "INFO Client: Application report for application_1700000000000_0001 (state: ACCEPTED)",
        "INFO Client: Application report for application_1700000000000_0001 (state: RUNNING)",
        "INFO Client: Application report for application_1700000000000_0001 (state: FINISHED)",
        "INFO Client: final status: SUCCEEDED",
    ]

    _RM_BASE_URL = "http://rm.test:8088"
    _RM_APP_ID = "application_1700000000000_0001"

    @classmethod
    def _rm_status_url(cls, app_id: str | None = None) -> str:
        return f"{cls._RM_BASE_URL}/ws/v1/cluster/apps/{app_id or cls._RM_APP_ID}"

    @classmethod
    def _rm_kill_url(cls, app_id: str | None = None) -> str:
        return f"{cls._RM_BASE_URL}/ws/v1/cluster/apps/{app_id or cls._RM_APP_ID}/state"

    @classmethod
    def _rm_status_resp(cls, final_status: str, state: str = "FINISHED") -> MagicMock:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = {"app": {"id": cls._RM_APP_ID, "state": state, "finalStatus": final_status}}
        return resp

    @staticmethod
    def _rm_failure_resp(status_code: int = 500, text: str = "Internal Server Error") -> MagicMock:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = status_code
        resp.text = text
        return resp

    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.put")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_default_keeps_existing_behavior_in_yarn_cluster(self, mock_popen, mock_get, mock_put):
        """Flag default False -> no HTTP calls; behavior identical to today."""
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        hook = SparkSubmitHook(conn_id="spark_yarn_cluster")
        hook.submit()

        proc.terminate.assert_not_called()
        mock_get.assert_not_called()
        mock_put.assert_not_called()
        assert hook._yarn_application_id == "application_1700000000000_0001"

    def test_yarn_status_tracking_requires_yarn_master(self):
        """yarn_track_via_rm_api=True should fail fast outside YARN."""
        hook = SparkSubmitHook(conn_id="spark_k8s_cluster", yarn_track_via_rm_api=True)

        with pytest.raises(ValueError, match="requires Spark master to be YARN"):
            hook.submit()

    def test_yarn_status_tracking_requires_cluster_deploy_mode(self):
        """yarn_track_via_rm_api=True should fail fast outside cluster deploy mode."""
        hook = SparkSubmitHook(
            conn_id="spark_yarn_rm",
            deploy_mode="client",
            yarn_track_via_rm_api=True,
        )

        with pytest.raises(ValueError, match="requires `deploy_mode='cluster'`"):
            hook.submit()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.time.sleep")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_succeeds(self, mock_popen, mock_get, mock_sleep):
        """RM returns UNDEFINED then SUCCEEDED -> hook returns normally."""
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        mock_get.side_effect = [
            self._rm_status_resp("UNDEFINED", state="RUNNING"),
            self._rm_status_resp("SUCCEEDED"),
        ]

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        hook.submit()

        spark_submit_cmd = mock_popen.call_args.args[0]
        assert "spark.yarn.submit.waitAppCompletion=false" in spark_submit_cmd
        proc.terminate.assert_not_called()
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(10)
        for call_obj in mock_get.call_args_list:
            assert call_obj.args[0] == self._rm_status_url()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.time.sleep")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_fails_on_killed(self, mock_popen, mock_get, mock_sleep):
        """RM returns KILLED -> raise with message containing app id and KILLED."""
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        mock_get.return_value = self._rm_status_resp("KILLED")

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        with pytest.raises(RuntimeError, match=f"{self._RM_APP_ID}.*KILLED"):
            hook.submit()
        proc.terminate.assert_not_called()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.time.sleep")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_fails_on_failed_state_with_undefined_final_status(
        self, mock_popen, mock_get, mock_sleep
    ):
        """RM state FAILED with finalStatus UNDEFINED should not poll forever."""
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        mock_get.return_value = self._rm_status_resp("UNDEFINED", state="FAILED")

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        with pytest.raises(RuntimeError, match=f"{self._RM_APP_ID}.*state: FAILED"):
            hook.submit()

        proc.terminate.assert_not_called()
        mock_sleep.assert_not_called()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.time.sleep")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_fails_on_unexpected_final_status(self, mock_popen, mock_get, mock_sleep):
        """RM returns a non-standard finalStatus ('BOGUS') -> raise without sleeping."""
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        mock_get.return_value = self._rm_status_resp("BOGUS")

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        with pytest.raises(RuntimeError, match="unexpected final status: BOGUS"):
            hook.submit()

        proc.terminate.assert_not_called()
        mock_sleep.assert_not_called()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_polls_without_application_submission_log(self, mock_popen, mock_get):
        """Missing 'Submitted application' log line should not block RM REST polling."""
        yarn_log_lines = [
            "INFO Client: Uploading resource file:/tmp/lib.zip -> "
            "hdfs://namenode:8020/user/root/.sparkStaging/application_1700000000000_0001/lib.zip",
            "INFO Client: Submitting application application_1700000000000_0001 to ResourceManager",
        ]
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(yarn_log_lines)
        proc.wait.return_value = 0
        mock_popen.return_value = proc
        mock_get.return_value = self._rm_status_resp("SUCCEEDED")

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        hook.submit()

        assert hook._yarn_application_id == self._RM_APP_ID
        assert mock_get.call_args.args[0] == self._rm_status_url()
        proc.terminate.assert_not_called()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_checks_spark_submit_exit_code_before_polling(self, mock_popen, mock_get):
        """spark-submit exits non-zero -> raise BEFORE issuing any HTTP request."""
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 1
        mock_popen.return_value = proc

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        with pytest.raises(AirflowException, match="Error code is: 1"):
            hook.submit()

        proc.terminate.assert_not_called()
        mock_get.assert_not_called()

    def test_yarn_status_tracking_rejects_conflicting_wait_app_completion_conf(self):
        """User-set spark.yarn.submit.waitAppCompletion=true conflicts with flag -> ValueError."""
        hook = SparkSubmitHook(
            conn_id="spark_yarn_rm",
            conf={"spark.yarn.submit.waitAppCompletion": "true"},
            yarn_track_via_rm_api=True,
        )

        with pytest.raises(ValueError, match="spark.yarn.submit.waitAppCompletion=false"):
            hook._build_spark_submit_command("")

    @patch("airflow.providers.apache.spark.hooks.spark_submit.time.sleep")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_tolerates_transient_failures(self, mock_popen, mock_get, mock_sleep):
        """3 consecutive 5xx responses then SUCCEEDED -> normal completion."""
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        # 3 transient failures (within the 10-failure budget), then SUCCEEDED.
        mock_get.side_effect = [
            self._rm_failure_resp(503, "Service Unavailable"),
            self._rm_failure_resp(502, "Bad Gateway"),
            self._rm_failure_resp(500, "Internal Server Error"),
            self._rm_status_resp("SUCCEEDED"),
        ]

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        hook.submit()

        assert mock_get.call_count == 4

    @patch("airflow.providers.apache.spark.hooks.spark_submit.time.sleep")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_tolerates_status_timeouts(self, mock_popen, mock_get, mock_sleep):
        """First requests.exceptions.Timeout, second call succeeds -> normal completion."""
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        mock_get.side_effect = [
            requests.exceptions.Timeout("read timed out"),
            self._rm_status_resp("SUCCEEDED"),
        ]

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        hook.submit()

        assert mock_get.call_count == 2
        # All calls must include the (connect, read) timeout tuple.
        for call_obj in mock_get.call_args_list:
            assert call_obj.kwargs["timeout"] == (5, 30)

    @patch("airflow.providers.apache.spark.hooks.spark_submit.time.sleep")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_raises_after_too_many_failures(self, mock_popen, mock_get, mock_sleep):
        """11 consecutive 5xx responses -> raise 'Giving up tracking YARN application'."""
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        # 11 failures: 10 tolerated; the 11th trips the budget.
        mock_get.side_effect = [self._rm_failure_resp() for _ in range(11)]

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        with pytest.raises(RuntimeError, match="Giving up tracking YARN application"):
            hook.submit()

        assert mock_get.call_count == 11

    @pytest.mark.parametrize("use_auth", [False, True])
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    def test_yarn_status_query_passes_auth_to_requests(self, mock_get, use_auth):
        """Explicit yarn_rm_auth is passed to requests.get, including the default None."""

        class _SentinelAuth(requests.auth.AuthBase):
            def __call__(self, r):
                return r

        auth = _SentinelAuth() if use_auth else None
        mock_get.return_value = self._rm_status_resp("SUCCEEDED")

        hook = SparkSubmitHook(
            conn_id="spark_yarn_rm",
            yarn_track_via_rm_api=True,
            yarn_rm_auth=auth,
        )
        hook._query_yarn_application_status(self._RM_APP_ID)

        assert mock_get.call_args.kwargs["auth"] is auth

    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    def test_yarn_status_query_uses_kerberos_auth_from_connection(self, mock_get):
        """Connection keytab + principal auto-enable HTTPKerberosAuth for RM requests."""

        class _SentinelKerberosAuth(requests.auth.AuthBase):
            def __call__(self, r):
                return r

        requests_kerberos = ModuleType("requests_kerberos")
        requests_kerberos.HTTPKerberosAuth = _SentinelKerberosAuth
        mock_get.return_value = self._rm_status_resp("SUCCEEDED")

        with (
            patch.object(
                SparkSubmitHook,
                "_create_keytab_path_from_base64_keytab",
                return_value="privileged_user.keytab",
            ),
            patch.dict("sys.modules", {"requests_kerberos": requests_kerberos}),
        ):
            hook = SparkSubmitHook(conn_id="spark_yarn_rm_kerberos", yarn_track_via_rm_api=True)
            hook._query_yarn_application_status(self._RM_APP_ID)

        assert isinstance(mock_get.call_args.kwargs["auth"], _SentinelKerberosAuth)

    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    def test_yarn_status_query_prefers_provided_auth_over_kerberos_connection(self, mock_get):
        """Explicit yarn_rm_auth stays an escape hatch even when Kerberos is configured."""

        class _SentinelAuth(requests.auth.AuthBase):
            def __call__(self, r):
                return r

        auth = _SentinelAuth()
        mock_get.return_value = self._rm_status_resp("SUCCEEDED")

        with (
            patch.object(
                SparkSubmitHook,
                "_create_keytab_path_from_base64_keytab",
                return_value="privileged_user.keytab",
            ),
            patch.dict("sys.modules", {"requests_kerberos": None}),
        ):
            hook = SparkSubmitHook(
                conn_id="spark_yarn_rm_kerberos",
                yarn_track_via_rm_api=True,
                yarn_rm_auth=auth,
            )
            hook._query_yarn_application_status(self._RM_APP_ID)

        assert mock_get.call_args.kwargs["auth"] is auth

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_fails_before_submit_when_kerberos_auth_dependency_missing(self, mock_popen):
        """Kerberos RM tracking requires requests-kerberos before spark-submit starts."""
        with patch.object(
            SparkSubmitHook,
            "_create_keytab_path_from_base64_keytab",
            return_value="privileged_user.keytab",
        ):
            hook = SparkSubmitHook(conn_id="spark_yarn_rm_kerberos", yarn_track_via_rm_api=True)

        with (
            patch.dict("sys.modules", {"requests_kerberos": None}),
            pytest.raises(RuntimeError, match="requests-kerberos"),
        ):
            hook.submit()

        mock_popen.assert_not_called()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_status_tracking_fails_before_submit_when_rm_url_missing(self, mock_popen):
        """Missing yarn_resourcemanager_webapp_address extra -> fail before spark-submit starts."""
        hook = SparkSubmitHook(conn_id="spark_yarn_cluster", yarn_track_via_rm_api=True)

        with pytest.raises(ValueError, match="yarn_resourcemanager_webapp_address"):
            hook.submit()

        mock_popen.assert_not_called()

    @patch("airflow.providers.apache.spark.hooks.spark_submit.time.sleep")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.get")
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_yarn_rm_base_url_is_resolved_once_across_polling_loop(self, mock_popen, mock_get, mock_sleep):
        """Connection lookup must run once even if the polling loop runs many iterations.

        Regression guard: a job polling every few seconds for hours must not re-fetch
        the Spark connection (and potentially re-hit a Secrets Backend) on every iteration.
        """
        proc = MagicMock(spec=["stdout", "terminate", "wait"])
        proc.stdout = iter(self._YARN_LOG_LINES)
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        # 4 UNDEFINED iterations then SUCCEEDED -> 5 polling iterations total.
        mock_get.side_effect = [
            self._rm_status_resp("UNDEFINED", state="RUNNING"),
            self._rm_status_resp("UNDEFINED", state="RUNNING"),
            self._rm_status_resp("UNDEFINED", state="RUNNING"),
            self._rm_status_resp("UNDEFINED", state="RUNNING"),
            self._rm_status_resp("SUCCEEDED"),
        ]

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        with patch.object(hook, "get_connection", wraps=hook.get_connection) as spy_get_conn:
            hook.submit()

        assert mock_get.call_count == 5
        assert spy_get_conn.call_count == 1

    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.put")
    def test_on_kill_sends_put_to_rm_when_app_id_known(self, mock_put):
        """_yarn_application_id known -> PUT {state: KILLED} to RM with configured auth."""

        class _SentinelAuth(requests.auth.AuthBase):
            def __call__(self, r):
                return r

        sentinel = _SentinelAuth()
        mock_put.return_value = MagicMock(spec=requests.Response, status_code=202)

        hook = SparkSubmitHook(
            conn_id="spark_yarn_rm",
            yarn_track_via_rm_api=True,
            yarn_rm_auth=sentinel,
        )
        hook._yarn_application_id = self._RM_APP_ID
        hook.on_kill()

        mock_put.assert_called_once()
        call_obj = mock_put.call_args
        assert call_obj.args[0] == self._rm_kill_url()
        assert call_obj.kwargs["json"] == {"state": "KILLED"}
        assert call_obj.kwargs["auth"] is sentinel

    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.put")
    def test_on_kill_uses_kerberos_auth_from_connection(self, mock_put):
        """Connection keytab + principal auto-enable HTTPKerberosAuth for RM kill requests."""

        class _SentinelKerberosAuth(requests.auth.AuthBase):
            def __call__(self, r):
                return r

        requests_kerberos = ModuleType("requests_kerberos")
        requests_kerberos.HTTPKerberosAuth = _SentinelKerberosAuth
        mock_put.return_value = MagicMock(spec=requests.Response, status_code=202)

        with (
            patch.object(
                SparkSubmitHook,
                "_create_keytab_path_from_base64_keytab",
                return_value="privileged_user.keytab",
            ),
            patch.dict("sys.modules", {"requests_kerberos": requests_kerberos}),
        ):
            hook = SparkSubmitHook(conn_id="spark_yarn_rm_kerberos", yarn_track_via_rm_api=True)
            hook._yarn_application_id = self._RM_APP_ID
            hook.on_kill()

        assert isinstance(mock_put.call_args.kwargs["auth"], _SentinelKerberosAuth)

    @patch("airflow.providers.apache.spark.hooks.spark_submit.requests.put")
    def test_on_kill_tolerates_rm_failure(self, mock_put):
        """RM PUT raises -> on_kill does not raise (best-effort, mirrors today)."""
        mock_put.side_effect = requests.exceptions.ConnectionError("RM unreachable")

        hook = SparkSubmitHook(conn_id="spark_yarn_rm", yarn_track_via_rm_api=True)
        hook._yarn_application_id = self._RM_APP_ID

        # Must not raise.
        hook.on_kill()

        mock_put.assert_called_once()
