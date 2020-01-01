# -*- coding: utf-8 -*-
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

import io
import unittest
from itertools import dropwhile
from unittest.mock import call, patch

from airflow.contrib.hooks.spark_sql_hook import SparkSqlHook
from airflow.models import Connection
from airflow.utils import db


def get_after(sentinel, iterable):
    """Get the value after `sentinel` in an `iterable`"""
    truncated = dropwhile(lambda el: el != sentinel, iterable)
    next(truncated)
    return next(truncated)


class TestSparkSqlHook(unittest.TestCase):
    _config = {
        'conn_id': 'spark_default',
        'executor_cores': 4,
        'executor_memory': '22g',
        'keytab': 'privileged_user.keytab',
        'name': 'spark-job',
        'num_executors': 10,
        'verbose': True,
        'sql': ' /path/to/sql/file.sql ',
        'conf': 'key=value,PROP=VALUE'
    }

    def setUp(self):

        db.merge_conn(
            Connection(
                conn_id='spark_default', conn_type='spark',
                host='yarn://yarn-master')
        )

    def test_build_command(self):
        hook = SparkSqlHook(**self._config)

        # The subprocess requires an array but we build the cmd by joining on a space
        cmd = ' '.join(hook._prepare_command(""))

        # Check all the parameters
        assert "--executor-cores {}".format(self._config['executor_cores']) in cmd
        assert "--executor-memory {}".format(self._config['executor_memory']) in cmd
        assert "--keytab {}".format(self._config['keytab']) in cmd
        assert "--name {}".format(self._config['name']) in cmd
        assert "--num-executors {}".format(self._config['num_executors']) in cmd
        sql_path = get_after('-f', hook._prepare_command(""))
        assert self._config['sql'].strip() == sql_path

        # Check if all config settings are there
        for key_value in self._config['conf'].split(","):
            k, v = key_value.split('=')
            assert "--conf {0}={1}".format(k, v) in cmd

        if self._config['verbose']:
            assert "--verbose" in cmd

    @patch('airflow.contrib.hooks.spark_sql_hook.subprocess.Popen')
    def test_spark_process_runcmd(self, mock_popen):
        # Given
        mock_popen.return_value.stdout = io.StringIO('Spark-sql communicates using stdout')
        mock_popen.return_value.stderr = io.StringIO('stderr')
        mock_popen.return_value.wait.return_value = 0

        # When
        hook = SparkSqlHook(
            conn_id='spark_default',
            sql='SELECT 1'
        )
        with patch.object(hook.log, 'debug') as mock_debug:
            with patch.object(hook.log, 'info') as mock_info:
                hook.run_query()
                mock_debug.assert_called_once_with(
                    'Spark-Sql cmd: %s',
                    ['spark-sql', '-e', 'SELECT 1', '--master', 'yarn', '--name', 'default-name', '--verbose',
                     '--queue', 'default']
                )
                mock_info.assert_called_once_with(
                    'Spark-sql communicates using stdout'
                )

        # Then
        self.assertEqual(
            mock_popen.mock_calls[0],
            call(['spark-sql', '-e', 'SELECT 1', '--master', 'yarn', '--name', 'default-name', '--verbose',
                  '--queue', 'default'], stderr=-2, stdout=-1)
        )


if __name__ == '__main__':
    unittest.main()
