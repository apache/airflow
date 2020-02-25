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

import os
import unittest
from unittest import mock

from airflow.configuration import conf
from airflow.models import TaskInstance
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils import timezone
from tests.providers.apache.hive import DEFAULT_DATE, TestHiveEnvironment


class HiveOperatorConfigTest(TestHiveEnvironment):

    def test_hive_airflow_default_config_queue(self):
        op = HiveOperator(
            task_id='test_default_config_queue',
            hql=self.hql,
            mapred_queue_priority='HIGH',
            mapred_job_name='airflow.test_default_config_queue',
            dag=self.dag)

        # just check that the correct default value in test_default.cfg is used
        test_config_hive_mapred_queue = conf.get(
            'hive',
            'default_hive_mapred_queue'
        )
        self.assertEqual(op.get_hook().mapred_queue, test_config_hive_mapred_queue)

    def test_hive_airflow_default_config_queue_override(self):
        specific_mapred_queue = 'default'
        op = HiveOperator(
            task_id='test_default_config_queue',
            hql=self.hql,
            mapred_queue=specific_mapred_queue,
            mapred_queue_priority='HIGH',
            mapred_job_name='airflow.test_default_config_queue',
            dag=self.dag)

        self.assertEqual(op.get_hook().mapred_queue, specific_mapred_queue)


class HiveOperatorTest(TestHiveEnvironment):

    def test_hiveconf_jinja_translate(self):
        hql = "SELECT ${num_col} FROM ${hiveconf:table};"
        op = HiveOperator(
            hiveconf_jinja_translate=True,
            task_id='dry_run_basic_hql', hql=hql, dag=self.dag)
        op.prepare_template()
        self.assertEqual(op.hql, "SELECT {{ num_col }} FROM {{ table }};")

    def test_hiveconf(self):
        hql = "SELECT * FROM ${hiveconf:table} PARTITION (${hiveconf:day});"
        op = HiveOperator(
            hiveconfs={'table': 'static_babynames', 'day': '{{ ds }}'},
            task_id='dry_run_basic_hql', hql=hql, dag=self.dag)
        op.prepare_template()
        self.assertEqual(
            op.hql,
            "SELECT * FROM ${hiveconf:table} PARTITION (${hiveconf:day});")

    @mock.patch('airflow.providers.apache.hive.operators.hive.HiveOperator.get_hook')
    def test_mapred_job_name(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_get_hook.return_value = mock_hook
        op = HiveOperator(
            task_id='test_mapred_job_name',
            hql=self.hql,
            dag=self.dag)

        fake_execution_date = timezone.datetime(2018, 6, 19)
        fake_ti = TaskInstance(task=op, execution_date=fake_execution_date)
        fake_ti.hostname = 'fake_hostname'
        fake_context = {'ti': fake_ti}

        op.execute(fake_context)
        self.assertEqual(
            "Airflow HiveOperator task for {}.{}.{}.{}"
            .format(fake_ti.hostname,
                    self.dag.dag_id, op.task_id,
                    fake_execution_date.isoformat()), mock_hook.mapred_job_name)


@unittest.skipIf(
    'AIRFLOW_RUNALL_TESTS' not in os.environ,
    "Skipped because AIRFLOW_RUNALL_TESTS is not set")
class TestHivePresto(TestHiveEnvironment):
    def test_hive(self):
        op = HiveOperator(
            task_id='basic_hql', hql=self.hql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_hive_queues(self):
        op = HiveOperator(
            task_id='test_hive_queues', hql=self.hql,
            mapred_queue='default', mapred_queue_priority='HIGH',
            mapred_job_name='airflow.test_hive_queues',
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_hive_dryrun(self):
        op = HiveOperator(
            task_id='dry_run_basic_hql', hql=self.hql, dag=self.dag)
        op.dry_run()

    def test_beeline(self):
        op = HiveOperator(
            task_id='beeline_hql', hive_cli_conn_id='hive_cli_default',
            hql=self.hql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)
