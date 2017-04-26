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
import sys
import unittest
from io import StringIO
from itertools import dropwhile

import mock

from airflow import configuration, models
from airflow.utils import db
from airflow.contrib.hooks.spark_sql_hook import SparkSqlHook


def get_after(sentinel, iterable):
    "Get the value after `sentinel` in an `iterable`"
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

        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
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
        for kv in self._config['conf'].split(","):
            k, v = kv.split('=')
            assert "--conf {0}={1}".format(k, v) in cmd

        if self._config['verbose']:
            assert "--verbose" in cmd


if __name__ == '__main__':
    unittest.main()
