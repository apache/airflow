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

import mock
import nose

from airflow import DAG, configuration, operators
from airflow.models import TaskInstance
from airflow.operators.hive_operator import HiveOperator
from airflow.utils import timezone

configuration.load_test_config()


DEFAULT_DATE = datetime.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


class HiveEnvironmentTest(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
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


class HiveOperatorConfigTest(HiveEnvironmentTest):

    def test_hive_airflow_default_config_queue(self):
        t = HiveOperator(
            task_id='test_default_config_queue',
            hql=self.hql,
            mapred_queue_priority='HIGH',
            mapred_job_name='airflow.test_default_config_queue',
            dag=self.dag)

        # just check that the correct default value in test_default.cfg is used
        test_config_hive_mapred_queue = configuration.conf.get(
            'hive',
            'default_hive_mapred_queue'
        )
        self.assertEqual(t.get_hook().mapred_queue, test_config_hive_mapred_queue)

    def test_hive_airflow_default_config_queue_override(self):
        specific_mapred_queue = 'default'
        t = HiveOperator(
            task_id='test_default_config_queue',
            hql=self.hql,
            mapred_queue=specific_mapred_queue,
            mapred_queue_priority='HIGH',
            mapred_job_name='airflow.test_default_config_queue',
            dag=self.dag)

        self.assertEqual(t.get_hook().mapred_queue, specific_mapred_queue)


class HiveOperatorTest(HiveEnvironmentTest):

    def test_hiveconf_jinja_translate(self):
        hql = "SELECT ${num_col} FROM ${hiveconf:table};"
        t = HiveOperator(
            hiveconf_jinja_translate=True,
            task_id='dry_run_basic_hql', hql=hql, dag=self.dag)
        t.prepare_template()
        self.assertEqual(t.hql, "SELECT {{ num_col }} FROM {{ table }};")

    def test_hiveconf(self):
        hql = "SELECT * FROM ${hiveconf:table} PARTITION (${hiveconf:day});"
        t = HiveOperator(
            hiveconfs={'table': 'static_babynames', 'day': '{{ ds }}'},
            task_id='dry_run_basic_hql', hql=hql, dag=self.dag)
        t.prepare_template()
        self.assertEqual(
            t.hql,
            "SELECT * FROM ${hiveconf:table} PARTITION (${hiveconf:day});")

    @mock.patch('airflow.operators.hive_operator.HiveOperator.get_hook')
    def test_mapred_job_name(self, mock_get_hook):
        mock_hook = mock.MagicMock()
        mock_get_hook.return_value = mock_hook
        t = HiveOperator(
            task_id='test_mapred_job_name',
            hql=self.hql,
            dag=self.dag)

        fake_execution_date = timezone.datetime(2018, 6, 19)
        fake_ti = TaskInstance(task=t, execution_date=fake_execution_date)
        fake_ti.hostname = 'fake_hostname'
        fake_context = {'ti': fake_ti}

        t.execute(fake_context)
        self.assertEqual(
            "Airflow HiveOperator task for {}.{}.{}.{}"
            .format(fake_ti.hostname,
                    self.dag.dag_id, t.task_id,
                    fake_execution_date.isoformat()), mock_hook.mapred_job_name)


if 'AIRFLOW_RUNALL_TESTS' in os.environ:

    import airflow.hooks.hive_hooks
    import airflow.operators.presto_to_mysql

    class HivePrestoTest(HiveEnvironmentTest):

        def test_hive(self):
            t = HiveOperator(
                task_id='basic_hql', hql=self.hql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_hive_queues(self):
            t = HiveOperator(
                task_id='test_hive_queues', hql=self.hql,
                mapred_queue='default', mapred_queue_priority='HIGH',
                mapred_job_name='airflow.test_hive_queues',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_hive_dryrun(self):
            t = HiveOperator(
                task_id='dry_run_basic_hql', hql=self.hql, dag=self.dag)
            t.dry_run()

        def test_beeline(self):
            t = HiveOperator(
                task_id='beeline_hql', hive_cli_conn_id='hive_cli_default',
                hql=self.hql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_presto(self):
            sql = """
            SELECT count(1) FROM airflow.static_babynames_partitioned;
            """
            t = operators.presto_check_operator.PrestoCheckOperator(
                task_id='presto_check', sql=sql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_presto_to_mysql(self):
            t = operators.presto_to_mysql.PrestoToMySqlTransfer(
                task_id='presto_to_mysql_check',
                sql="""
                SELECT name, count(*) as ccount
                FROM airflow.static_babynames
                GROUP BY name
                """,
                mysql_table='test_static_babynames',
                mysql_preoperator='TRUNCATE TABLE test_static_babynames;',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_hdfs_sensor(self):
            t = operators.sensors.HdfsSensor(
                task_id='hdfs_sensor_check',
                filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_webhdfs_sensor(self):
            t = operators.sensors.WebHdfsSensor(
                task_id='webhdfs_sensor_check',
                filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
                timeout=120,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_sql_sensor(self):
            t = operators.sensors.SqlSensor(
                task_id='hdfs_sensor_check',
                conn_id='presto_default',
                sql="SELECT 'x' FROM airflow.static_babynames LIMIT 1;",
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_hive_stats(self):
            t = operators.hive_stats_operator.HiveStatsCollectionOperator(
                task_id='hive_stats_check',
                table="airflow.static_babynames_partitioned",
                partition={'ds': DEFAULT_DATE_DS},
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_named_hive_partition_sensor(self):
            t = operators.sensors.NamedHivePartitionSensor(
                task_id='hive_partition_check',
                partition_names=[
                    "airflow.static_babynames_partitioned/ds={{ds}}"
                ],
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_named_hive_partition_sensor_succeeds_on_multiple_partitions(self):
            t = operators.sensors.NamedHivePartitionSensor(
                task_id='hive_partition_check',
                partition_names=[
                    "airflow.static_babynames_partitioned/ds={{ds}}",
                    "airflow.static_babynames_partitioned/ds={{ds}}"
                ],
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_named_hive_partition_sensor_parses_partitions_with_periods(self):
            t = operators.sensors.NamedHivePartitionSensor.parse_partition_name(
                partition="schema.table/part1=this.can.be.an.issue/part2=ok")
            self.assertEqual(t[0], "schema")
            self.assertEqual(t[1], "table")
            self.assertEqual(t[2], "part1=this.can.be.an.issue/part2=this_should_be_ok")

        @nose.tools.raises(airflow.exceptions.AirflowSensorTimeout)
        def test_named_hive_partition_sensor_times_out_on_nonexistent_partition(self):
            t = operators.sensors.NamedHivePartitionSensor(
                task_id='hive_partition_check',
                partition_names=[
                    "airflow.static_babynames_partitioned/ds={{ds}}",
                    "airflow.static_babynames_partitioned/ds=nonexistent"
                ],
                poke_interval=0.1,
                timeout=1,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_hive_partition_sensor(self):
            t = operators.sensors.HivePartitionSensor(
                task_id='hive_partition_check',
                table='airflow.static_babynames_partitioned',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_hive_metastore_sql_sensor(self):
            t = operators.sensors.MetastorePartitionSensor(
                task_id='hive_partition_check',
                table='airflow.static_babynames_partitioned',
                partition_name='ds={}'.format(DEFAULT_DATE_DS),
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_hive2samba(self):
            t = operators.hive_to_samba_operator.Hive2SambaOperator(
                task_id='hive2samba_check',
                samba_conn_id='tableau_samba',
                hql="SELECT * FROM airflow.static_babynames LIMIT 10000",
                destination_filepath='test_airflow.csv',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)

        def test_hive_to_mysql(self):
            t = operators.hive_to_mysql.HiveToMySqlTransfer(
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
            t.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                  ignore_ti_state=True)
