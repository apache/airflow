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

from __future__ import print_function

import datetime
import os
import unittest
import six

from airflow import DAG, configuration, operators, utils
configuration.test_mode()

import os
import unittest
import nose


DEFAULT_DATE = datetime.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


if 'AIRFLOW_RUNALL_TESTS' in os.environ:

    import airflow.hooks.hive_hooks
    import airflow.operators.presto_to_mysql

    class HiveServer2Test(unittest.TestCase):
        def setUp(self):
            configuration.test_mode()

        def test_select_conn(self):
            from airflow.hooks.hive_hooks import HiveServer2Hook
            sql = "select 1"
            hook = HiveServer2Hook()
            hook.get_records(sql)

        def test_multi_statements(self):
            from airflow.hooks.hive_hooks import HiveServer2Hook
            sqls = [
                "CREATE TABLE IF NOT EXISTS test_multi_statements (i INT)",
                "DROP TABLE test_multi_statements",
            ]
            hook = HiveServer2Hook()
            hook.get_records(sqls)

        def test_get_metastore_databases(self):
            if six.PY2:
                from airflow.hooks.hive_hooks import HiveMetastoreHook
                hook = HiveMetastoreHook()
                hook.get_databases()

        def test_to_csv(self):
            from airflow.hooks.hive_hooks import HiveServer2Hook
            sql = "select 1"
            hook = HiveServer2Hook()
            hook.to_csv(hql=sql, csv_filepath="/tmp/test_to_csv")

    class HivePrestoTest(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
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

        def test_hive(self):
            import airflow.operators.hive_operator
            t = operators.hive_operator.HiveOperator(
                task_id='basic_hql', hql=self.hql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_queues(self):
            import airflow.operators.hive_operator
            t = operators.hive_operator.HiveOperator(
                task_id='test_hive_queues', hql=self.hql,
                mapred_queue='default', mapred_queue_priority='HIGH',
                mapred_job_name='airflow.test_hive_queues',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


        def test_hive_dryrun(self):
            import airflow.operators.hive_operator
            t = operators.hive_operator.HiveOperator(
                task_id='dry_run_basic_hql', hql=self.hql, dag=self.dag)
            t.dry_run()

        def test_beeline(self):
            import airflow.operators.hive_operator
            t = operators.hive_operator.HiveOperator(
                task_id='beeline_hql', hive_cli_conn_id='beeline_default',
                hql=self.hql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_presto(self):
            sql = """
            SELECT count(1) FROM airflow.static_babynames_partitioned;
            """
            import airflow.operators.presto_check_operator
            t = operators.presto_check_operator.PrestoCheckOperator(
                task_id='presto_check', sql=sql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_presto_to_mysql(self):
            import airflow.operators.presto_to_mysql
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
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hdfs_sensor(self):
            t = operators.sensors.HdfsSensor(
                task_id='hdfs_sensor_check',
                filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_webhdfs_sensor(self):
            t = operators.sensors.WebHdfsSensor(
                task_id='webhdfs_sensor_check',
                filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
                timeout=120,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_sql_sensor(self):
            t = operators.sensors.SqlSensor(
                task_id='hdfs_sensor_check',
                conn_id='presto_default',
                sql="SELECT 'x' FROM airflow.static_babynames LIMIT 1;",
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_stats(self):
            import airflow.operators.hive_stats_operator
            t = operators.hive_stats_operator.HiveStatsCollectionOperator(
                task_id='hive_stats_check',
                table="airflow.static_babynames_partitioned",
                partition={'ds': DEFAULT_DATE_DS},
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_named_hive_partition_sensor(self):
            t = operators.sensors.NamedHivePartitionSensor(
                task_id='hive_partition_check',
                partition_names=["airflow.static_babynames_partitioned/ds={{ds}}"],
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_named_hive_partition_sensor_succeeds_on_multiple_partitions(self):
            t = operators.sensors.NamedHivePartitionSensor(
                task_id='hive_partition_check',
                partition_names=[
                    "airflow.static_babynames_partitioned/ds={{ds}}",
                    "airflow.static_babynames_partitioned/ds={{ds}}"
                ],
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

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
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_partition_sensor(self):
            t = operators.sensors.HivePartitionSensor(
                task_id='hive_partition_check',
                table='airflow.static_babynames_partitioned',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_metastore_sql_sensor(self):
            t = operators.sensors.MetastorePartitionSensor(
                task_id='hive_partition_check',
                table='airflow.static_babynames_partitioned',
                partition_name='ds={}'.format(DEFAULT_DATE_DS),
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive2samba(self):
            import airflow.operators.hive_to_samba_operator
            t = operators.hive_to_samba_operator.Hive2SambaOperator(
                task_id='hive2samba_check',
                samba_conn_id='tableau_samba',
                hql="SELECT * FROM airflow.static_babynames LIMIT 10000",
                destination_filepath='test_airflow.csv',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_to_mysql(self):
            import airflow.operators.hive_to_mysql
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
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)
