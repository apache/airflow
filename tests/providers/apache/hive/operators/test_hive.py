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

import datetime
import os
import unittest
from unittest import mock

from airflow import DAG
from airflow.configuration import conf
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import TaskInstance
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.hive_to_samba_operator import Hive2SambaOperator
from airflow.operators.presto_to_mysql import PrestoToMySqlTransfer
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.hive.operators.hive_stats import HiveStatsCollectionOperator
from airflow.providers.apache.hive.sensors.hive_partition import HivePartitionSensor
from airflow.providers.apache.hive.sensors.metastore_partition import MetastorePartitionSensor
from airflow.providers.apache.hive.sensors.named_hive_partition import NamedHivePartitionSensor
from airflow.providers.presto.operators.presto_check import PrestoCheckOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils import timezone

DEFAULT_DATE = datetime.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


class TestHiveEnvironment(unittest.TestCase):

    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG('test_dag_id', default_args=args)
        self.dag = dag
        self.hql = """
        USE airflow;
        DROP TABLE IF EXISTS static_babynames_partitioned;
        CREATE TABLE IF NOT EXISTS static_babynames_partitioned (
            state string,
            year string,
            name string,
            gender string,
            num int)
        PARTITIONED BY (ds string);
        INSERT OVERWRITE TABLE static_babynames_partitioned
            PARTITION(ds='{{ ds }}')
        SELECT state, year, name, gender, num FROM static_babynames;
        """


class TestHiveCli(unittest.TestCase):

    def setUp(self):
        self.nondefault_schema = "nondefault"
        os.environ["AIRFLOW__CORE__SECURITY"] = "kerberos"

    def tearDown(self):
        del os.environ["AIRFLOW__CORE__SECURITY"]

    def test_get_proxy_user_value(self):
        from airflow.providers.apache.hive.hooks.hive import HiveCliHook

        hook = HiveCliHook()
        returner = mock.MagicMock()
        returner.extra_dejson = {'proxy_user': 'a_user_proxy'}
        hook.use_beeline = True
        hook.conn = returner

        # Run
        result = hook._prepare_cli_cmd()

        # Verify
        self.assertIn('hive.server2.proxy.user=a_user_proxy', result[2])


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

    def test_presto(self):
        sql = """
            SELECT count(1) FROM airflow.static_babynames_partitioned;
            """
        op = PrestoCheckOperator(
            task_id='presto_check', sql=sql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_presto_to_mysql(self):
        op = PrestoToMySqlTransfer(
            task_id='presto_to_mysql_check',
            sql="""
                SELECT name, count(*) as ccount
                FROM airflow.static_babynames
                GROUP BY name
                """,
            mysql_table='test_static_babynames',
            mysql_preoperator='TRUNCATE TABLE test_static_babynames;',
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_hdfs_sensor(self):
        op = HdfsSensor(
            task_id='hdfs_sensor_check',
            filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_webhdfs_sensor(self):
        op = WebHdfsSensor(
            task_id='webhdfs_sensor_check',
            filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
            timeout=120,
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_sql_sensor(self):
        op = SqlSensor(
            task_id='hdfs_sensor_check',
            conn_id='presto_default',
            sql="SELECT 'x' FROM airflow.static_babynames LIMIT 1;",
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_hive_stats(self):
        op = HiveStatsCollectionOperator(
            task_id='hive_stats_check',
            table="airflow.static_babynames_partitioned",
            partition={'ds': DEFAULT_DATE_DS},
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_named_hive_partition_sensor(self):
        op = NamedHivePartitionSensor(
            task_id='hive_partition_check',
            partition_names=[
                "airflow.static_babynames_partitioned/ds={{ds}}"
            ],
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_named_hive_partition_sensor_succeeds_on_multiple_partitions(self):
        op = NamedHivePartitionSensor(
            task_id='hive_partition_check',
            partition_names=[
                "airflow.static_babynames_partitioned/ds={{ds}}",
                "airflow.static_babynames_partitioned/ds={{ds}}"
            ],
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_named_hive_partition_sensor_parses_partitions_with_periods(self):
        name = NamedHivePartitionSensor.parse_partition_name(
            partition="schema.table/part1=this.can.be.an.issue/part2=ok")
        self.assertEqual(name[0], "schema")
        self.assertEqual(name[1], "table")
        self.assertEqual(name[2], "part1=this.can.be.an.issue/part2=this_should_be_ok")

    def test_named_hive_partition_sensor_times_out_on_nonexistent_partition(self):
        with self.assertRaises(AirflowSensorTimeout):
            op = NamedHivePartitionSensor(
                task_id='hive_partition_check',
                partition_names=[
                    "airflow.static_babynames_partitioned/ds={{ds}}",
                    "airflow.static_babynames_partitioned/ds=nonexistent"
                ],
                poke_interval=0.1,
                timeout=1,
                dag=self.dag)
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                   ignore_ti_state=True)

    def test_hive_partition_sensor(self):
        op = HivePartitionSensor(
            task_id='hive_partition_check',
            table='airflow.static_babynames_partitioned',
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_hive_metastore_sql_sensor(self):
        op = MetastorePartitionSensor(
            task_id='hive_partition_check',
            table='airflow.static_babynames_partitioned',
            partition_name='ds={}'.format(DEFAULT_DATE_DS),
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_hive2samba(self):
        op = Hive2SambaOperator(
            task_id='hive2samba_check',
            samba_conn_id='tableau_samba',
            hql="SELECT * FROM airflow.static_babynames LIMIT 10000",
            destination_filepath='test_airflow.csv',
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def test_hive_to_mysql(self):
        op = HiveToMySqlTransfer(
            mysql_conn_id='airflow_db',
            task_id='hive_to_mysql_check',
            create=True,
            sql="""
                SELECT name
                FROM airflow.static_babynames
                LIMIT 100
                """,
            mysql_table='test_static_babynames',
            mysql_preoperator=[
                'DROP TABLE IF EXISTS test_static_babynames;',
                'CREATE TABLE test_static_babynames (name VARCHAR(500))',
            ],
            dag=self.dag)
        op.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)
