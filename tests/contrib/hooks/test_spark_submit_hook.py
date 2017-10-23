# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import six
import sys
import unittest

from airflow import configuration, models
from airflow.utils import db
from mock import patch, call

from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook


class TestSparkSubmitHook(unittest.TestCase):

    _spark_job_file = 'test_application.py'
    _config = {
        'conf': {
            'parquet.compression': 'SNAPPY'
        },
        'conn_id': 'default_spark',
        'files': 'hive-site.xml',
        'py_files': 'sample_library.py',
        'jars': 'parquet.jar',
        'packages': 'com.databricks:spark-avro_2.11:3.2.0',
        'total_executor_cores': 4,
        'executor_cores': 4,
        'executor_memory': '22g',
        'keytab': 'privileged_user.keytab',
        'principal': 'user/spark@airflow.org',
        'name': 'spark-job',
        'num_executors': 10,
        'verbose': True,
        'driver_memory': '3g',
        'java_class': 'com.foo.bar.AppMain',
        'application_args': [
            '-f', 'foo',
            '--bar', 'bar',
            '--with-spaces', 'args should keep embdedded spaces',
            'baz'
        ]
    }

    @staticmethod
    def cmd_args_to_dict(list_cmd):
        return_dict = {}
        for arg in list_cmd:
            if arg.startswith("--"):
                pos = list_cmd.index(arg)
                return_dict[arg] = list_cmd[pos+1]
        return return_dict

    def setUp(self):

        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='spark_yarn_cluster', conn_type='spark',
                host='yarn://yarn-master', extra='{"queue": "root.etl", "deploy-mode": "cluster"}')
        )
        db.merge_conn(
            models.Connection(
                conn_id='spark_default_mesos', conn_type='spark',
                host='mesos://host', port=5050)
        )

        db.merge_conn(
            models.Connection(
                conn_id='spark_home_set', conn_type='spark',
                host='yarn://yarn-master',
                extra='{"spark-home": "/opt/myspark"}')
        )

        db.merge_conn(
            models.Connection(
                conn_id='spark_home_not_set', conn_type='spark',
                host='yarn://yarn-master')
        )
        db.merge_conn(
            models.Connection(
                conn_id='spark_binary_set', conn_type='spark',
                host='yarn', extra='{"spark-binary": "custom-spark-submit"}')
        )
        db.merge_conn(
            models.Connection(
                conn_id='spark_binary_and_home_set', conn_type='spark',
                host='yarn', extra='{"spark-home": "/path/to/spark_home", "spark-binary": "custom-spark-submit"}')
        )

    def test_build_command(self):
        # Given
        hook = SparkSubmitHook(**self._config)

        # When
        cmd = hook._build_command(self._spark_job_file)

        # Then
        expected_build_cmd = [
            'spark-submit',
            '--master', 'yarn',
            '--conf', 'parquet.compression=SNAPPY',
            '--files', 'hive-site.xml',
            '--py-files', 'sample_library.py',
            '--jars', 'parquet.jar',
            '--packages', 'com.databricks:spark-avro_2.11:3.2.0',
            '--num-executors', '10',
            '--total-executor-cores', '4',
            '--executor-cores', '4',
            '--executor-memory', '22g',
            '--driver-memory', '3g',
            '--keytab', 'privileged_user.keytab',
            '--principal', 'user/spark@airflow.org',
            '--name', 'spark-job',
            '--class', 'com.foo.bar.AppMain',
            '--verbose',
            'test_application.py',
            '-f', 'foo',
            '--bar', 'bar',
            '--with-spaces', 'args should keep embdedded spaces',
            'baz'
        ]
        self.assertEquals(expected_build_cmd, cmd)

    @patch('airflow.contrib.hooks.spark_submit_hook.subprocess.Popen')
    def test_spark_process_runcmd(self, mock_popen):
        # Given
        mock_popen.return_value.stdout = six.StringIO('stdout')
        mock_popen.return_value.stderr = six.StringIO('stderr')
        mock_popen.return_value.wait.return_value = 0

        # When
        hook = SparkSubmitHook(conn_id='')
        hook.submit()

        # Then
        self.assertEqual(mock_popen.mock_calls[0], call(['spark-submit', '--master', 'yarn', '--name', 'default-name', ''], stderr=-2, stdout=-1, universal_newlines=True, bufsize=-1))

    def test_resolve_connection_yarn_default(self):
        # Given
        hook = SparkSubmitHook(conn_id='')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(dict_cmd["--master"], "yarn")

    def test_resolve_connection_yarn_default_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_default')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": "root.default",
                                     "spark_home": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(dict_cmd["--master"], "yarn")
        self.assertEqual(dict_cmd["--queue"], "root.default")

    def test_resolve_connection_mesos_default_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_default_mesos')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {"master": "mesos://host:5050",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(dict_cmd["--master"], "mesos://host:5050")

    def test_resolve_connection_spark_yarn_cluster_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_yarn_cluster')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {"master": "yarn://yarn-master",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": "cluster",
                                     "queue": "root.etl",
                                     "spark_home": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(dict_cmd["--master"], "yarn://yarn-master")
        self.assertEqual(dict_cmd["--queue"], "root.etl")
        self.assertEqual(dict_cmd["--deploy-mode"], "cluster")

    def test_resolve_connection_spark_home_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_home_set')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn://yarn-master",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": "/opt/myspark"}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], '/opt/myspark/bin/spark-submit')

    def test_resolve_connection_spark_home_not_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_home_not_set')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn://yarn-master",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], 'spark-submit')

    def test_resolve_connection_spark_binary_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_binary_set')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "custom-spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], 'custom-spark-submit')

    def test_resolve_connection_spark_binary_and_home_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_binary_and_home_set')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "custom-spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": "/path/to/spark_home"}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], '/path/to/spark_home/bin/custom-spark-submit')

    def test_process_log(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_yarn_cluster')
        log_lines = [
            'SPARK_MAJOR_VERSION is set to 2, using Spark2',
            'WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable',
            'WARN DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.',
            'INFO Client: Requesting a new application from cluster with 10 NodeManagers',
            'INFO Client: Submitting application application_1486558679801_1820 to ResourceManager'
        ]
        # When
        hook._process_log(log_lines)

        # Then

        self.assertEqual(hook._yarn_application_id, 'application_1486558679801_1820')

    @patch('airflow.contrib.hooks.spark_submit_hook.subprocess.Popen')
    def test_spark_process_on_kill(self, mock_popen):
        # Given
        mock_popen.return_value.stdout = six.StringIO('stdout')
        mock_popen.return_value.stderr = six.StringIO('stderr')
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0
        log_lines = [
            'SPARK_MAJOR_VERSION is set to 2, using Spark2',
            'WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable',
            'WARN DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.',
            'INFO Client: Requesting a new application from cluster with 10 NodeManagerapplication_1486558679801_1820s',
            'INFO Client: Submitting application application_1486558679801_1820 to ResourceManager'
        ]
        hook = SparkSubmitHook(conn_id='spark_yarn_cluster')
        hook._process_log(log_lines)
        hook.submit()

        # When
        hook.on_kill()

        # Then
        self.assertIn(call(['yarn', 'application', '-kill', 'application_1486558679801_1820'], stderr=-1, stdout=-1), mock_popen.mock_calls)


if __name__ == '__main__':
    unittest.main()
