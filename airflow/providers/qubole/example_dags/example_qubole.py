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

import filecmp
import random
import textwrap

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.qubole.operators.qubole import QuboleOperator
from airflow.providers.qubole.sensors.qubole import QuboleFileSensor, QubolePartitionSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='example_qubole_operator',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    dag.doc_md = textwrap.dedent(
        """
        This is only an example DAG to highlight usage of QuboleOperator in various scenarios,
        some of these tasks may or may not work based on your Qubole account setup.

        Run a shell command from Qubole Analyze against your Airflow cluster with following to
        trigger it manually `airflow dags trigger example_qubole_operator`.

        *Note: Make sure that connection `qubole_default` is properly set before running this
        example. Also be aware that it might spin up clusters to run these examples.*
        """
    )

    def compare_result_fn(**kwargs):
        """
        Compares the results of two QuboleOperator tasks.

        :param kwargs: The context of the executed task.
        :type kwargs: dict
        :return: True if the files are the same, False otherwise.
        :rtype: bool
        """
        ti = kwargs['ti']
        qubole_result_1 = hive_show_table.get_results(ti)
        qubole_result_2 = hive_s3_location.get_results(ti)
        return filecmp.cmp(qubole_result_1, qubole_result_2)

    hive_show_table = QuboleOperator(
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
        params={
            'cluster_label': 'default',
        },
    )

    hive_s3_location = QuboleOperator(
        task_id='hive_s3_location',
        command_type="hivecmd",
        script_location="s3n://public-qubole/qbol-library/scripts/show_table.hql",
        notify=True,
        tags=['tag1', 'tag2'],
        # If the script at s3 location has any qubole specific macros to be replaced
        # macros='[{"date": "{{ ds }}"}, {"name" : "abc"}]',
        trigger_rule="all_done",
    )

    compare_result = PythonOperator(
        task_id='compare_result', python_callable=compare_result_fn, trigger_rule="all_done"
    )

    compare_result << [hive_show_table, hive_s3_location]

    options = ['hadoop_jar_cmd', 'presto_cmd', 'db_query', 'spark_cmd']

    branching = BranchPythonOperator(task_id='branching', python_callable=lambda: random.choice(options))

    branching << compare_result

    join = DummyOperator(task_id='join', trigger_rule='one_success')

    hadoop_jar_cmd = QuboleOperator(
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
        params={
            'cluster_label': 'default',
        },
    )

    pig_cmd = QuboleOperator(
        task_id='pig_cmd',
        command_type="pigcmd",
        script_location="s3://public-qubole/qbol-library/scripts/script1-hadoop-s3-small.pig",
        parameters="key1=value1 key2=value2",
        trigger_rule="all_done",
    )

    pig_cmd << hadoop_jar_cmd << branching
    pig_cmd >> join

    presto_cmd = QuboleOperator(task_id='presto_cmd', command_type='prestocmd', query='show tables')

    shell_cmd = QuboleOperator(
        task_id='shell_cmd',
        command_type="shellcmd",
        script_location="s3://public-qubole/qbol-library/scripts/shellx.sh",
        parameters="param1 param2",
        trigger_rule="all_done",
    )

    shell_cmd << presto_cmd << branching
    shell_cmd >> join

    db_query = QuboleOperator(
        task_id='db_query', command_type='dbtapquerycmd', query='show tables', db_tap_id=2064
    )

    db_export = QuboleOperator(
        task_id='db_export',
        command_type='dbexportcmd',
        mode=1,
        hive_table='default_qubole_airline_origin_destination',
        db_table='exported_airline_origin_destination',
        partition_spec='dt=20110104-02',
        dbtap_id=2064,
        trigger_rule="all_done",
    )

    db_export << db_query << branching
    db_export >> join

    db_import = QuboleOperator(
        task_id='db_import',
        command_type='dbimportcmd',
        mode=1,
        hive_table='default_qubole_airline_origin_destination',
        db_table='exported_airline_origin_destination',
        where_clause='id < 10',
        parallelism=2,
        dbtap_id=2064,
        trigger_rule="all_done",
    )

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

    spark_cmd = QuboleOperator(
        task_id='spark_cmd',
        command_type="sparkcmd",
        program=prog,
        language='scala',
        arguments='--class SparkPi',
        tags='airflow_example_run',
    )

    spark_cmd << db_import << branching
    spark_cmd >> join

with DAG(
    dag_id='example_qubole_sensor',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    doc_md=__doc__,
    tags=['example'],
) as dag2:
    dag2.doc_md = textwrap.dedent(
        """
        This is only an example DAG to highlight usage of QuboleSensor in various scenarios,
        some of these tasks may or may not work based on your QDS account setup.

        Run a shell command from Qubole Analyze against your Airflow cluster with following to
        trigger it manually `airflow dags trigger example_qubole_sensor`.

        *Note: Make sure that connection `qubole_default` is properly set before running
        this example.*
        """
    )

    check_s3_file = QuboleFileSensor(
        task_id='check_s3_file',
        qubole_conn_id='qubole_default',
        poke_interval=60,
        timeout=600,
        data={
            "files": [
                "s3://paid-qubole/HadoopAPIExamples/jars/hadoop-0.20.1-dev-streaming.jar",
                "s3://paid-qubole/HadoopAPITests/data/{{ ds.split('-')[2] }}.tsv",
            ]  # will check for availability of all the files in array
        },
    )

    check_hive_partition = QubolePartitionSensor(
        task_id='check_hive_partition',
        poke_interval=10,
        timeout=60,
        data={
            "schema": "default",
            "table": "my_partitioned_table",
            "columns": [
                {"column": "month", "values": ["{{ ds.split('-')[1] }}"]},
                {"column": "day", "values": ["{{ ds.split('-')[2] }}", "{{ yesterday_ds.split('-')[2] }}"]},
            ],  # will check for partitions like [month=12/day=12,month=12/day=13]
        },
    )

    check_s3_file >> check_hive_partition
