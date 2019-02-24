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

"""
This is only an example DAG to highlight usage of QuboleOperator in various scenarios,
some of these tasks may or may not work based on your Qubole account setup.

Run a shell command from Qubole Analyze against your Airflow cluster with following to
trigger it manually `airflow trigger_dag example_qubole_operator`.

*Note: Make sure that connection `qubole_default` is properly set before running this
example. Also be aware that it might spin up clusters to run these examples.*
"""

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.qubole_operator import QuboleOperator
import filecmp
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG('example_qubole_operator', default_args=default_args, schedule_interval=None)

dag.doc_md = __doc__


def compare_result(ds, **kwargs):
    ti = kwargs['ti']
    r1 = t1.get_results(ti)
    r2 = t2.get_results(ti)
    return filecmp.cmp(r1, r2)


t1 = QuboleOperator(
    task_id='hive_show_table',
    command_type='hivecmd',
    query='show tables',
    cluster_label='{{ params.cluster_label }}',
    fetch_logs=True,
    # If `fetch_logs`=true, will fetch qubole command logs and concatenate
    # them into corresponding airflow task logs
    tags='airflow_example_run',
    # To attach tags to qubole command, auto attach 3 tags - dag_id, task_id, run_id
    qubole_conn_id='qubole_default',
    # Connection id to submit commands inside QDS, if not set "qubole_default" is used
    dag=dag,
    params={
        'cluster_label': 'default',
    }
)

t2 = QuboleOperator(
    task_id='hive_s3_location',
    command_type="hivecmd",
    script_location="s3n://public-qubole/qbol-library/scripts/show_table.hql",
    notfiy=True,
    tags=['tag1', 'tag2'],
    # If the script at s3 location has any qubole specific macros to be replaced
    # macros='[{"date": "{{ ds }}"}, {"name" : "abc"}]',
    trigger_rule="all_done",
    dag=dag)

t3 = PythonOperator(
    task_id='compare_result',
    provide_context=True,
    python_callable=compare_result,
    trigger_rule="all_done",
    dag=dag)

t3.set_upstream(t1)
t3.set_upstream(t2)

options = ['hadoop_jar_cmd', 'presto_cmd', 'db_query', 'spark_cmd']

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: random.choice(options),
    dag=dag)
branching.set_upstream(t3)

join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag
)

t4 = QuboleOperator(
    task_id='hadoop_jar_cmd',
    command_type='hadoopcmd',
    sub_command='jar s3://paid-qubole/HadoopAPIExamples/'
                'jars/hadoop-0.20.1-dev-streaming.jar '
                '-mapper wc '
                '-numReduceTasks 0 -input s3://paid-qubole/HadoopAPITests/'
                'data/3.tsv -output '
                's3://paid-qubole/HadoopAPITests/data/3_wc',
    cluster_label='{{ params.cluster_label }}',
    fetch_logs=True,
    dag=dag,
    params={
        'cluster_label': 'default',
    }
)

t5 = QuboleOperator(
    task_id='pig_cmd',
    command_type="pigcmd",
    script_location="s3://public-qubole/qbol-library/scripts/script1-hadoop-s3-small.pig",
    parameters="key1=value1 key2=value2",
    trigger_rule="all_done",
    dag=dag)

t4.set_upstream(branching)
t5.set_upstream(t4)
t5.set_downstream(join)

t6 = QuboleOperator(
    task_id='presto_cmd',
    command_type='prestocmd',
    query='show tables',
    dag=dag)

t7 = QuboleOperator(
    task_id='shell_cmd',
    command_type="shellcmd",
    script_location="s3://public-qubole/qbol-library/scripts/shellx.sh",
    parameters="param1 param2",
    trigger_rule="all_done",
    dag=dag)

t6.set_upstream(branching)
t7.set_upstream(t6)
t7.set_downstream(join)

t8 = QuboleOperator(
    task_id='db_query',
    command_type='dbtapquerycmd',
    query='show tables',
    db_tap_id=2064,
    dag=dag)

t9 = QuboleOperator(
    task_id='db_export',
    command_type='dbexportcmd',
    mode=1,
    hive_table='default_qubole_airline_origin_destination',
    db_table='exported_airline_origin_destination',
    partition_spec='dt=20110104-02',
    dbtap_id=2064,
    trigger_rule="all_done",
    dag=dag)

t8.set_upstream(branching)
t9.set_upstream(t8)
t9.set_downstream(join)

t10 = QuboleOperator(
    task_id='db_import',
    command_type='dbimportcmd',
    mode=1,
    hive_table='default_qubole_airline_origin_destination',
    db_table='exported_airline_origin_destination',
    where_clause='id < 10',
    db_parallelism=2,
    dbtap_id=2064,
    trigger_rule="all_done",
    dag=dag)

prog = '''
import scala.math.random

import org.apache.spark._

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
'''

t11 = QuboleOperator(
    task_id='spark_cmd',
    command_type="sparkcmd",
    program=prog,
    language='scala',
    arguments='--class SparkPi',
    tags='airflow_example_run',
    dag=dag)

t11.set_upstream(branching)
t11.set_downstream(t10)
t10.set_downstream(join)
