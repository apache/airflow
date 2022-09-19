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

import filecmp
import os
import random
import textwrap
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

from airflow.operators.python import BranchPythonOperator
from airflow.providers.qubole.operators.qubole import QuboleOperator
from airflow.utils.trigger_rule import TriggerRule

START_DATE = datetime(2021, 1, 1)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_qubole_operator"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=START_DATE,
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

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def compare_result(hive_show_table, hive_s3_location, ti=None):
        """
        Compares the results of two QuboleOperator tasks.

        :param hive_show_table: The "hive_show_table" task.
        :param hive_s3_location: The "hive_s3_location" task.
        :param ti: The TaskInstance object.
        :return: True if the files are the same, False otherwise.
        :rtype: bool
        """
        qubole_result_1 = hive_show_table.get_results(ti)
        qubole_result_2 = hive_s3_location.get_results(ti)
        return filecmp.cmp(qubole_result_1, qubole_result_2)

    # [START howto_operator_qubole_run_hive_query]
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
        params={
            'cluster_label': 'default',
        },
    )
    # [END howto_operator_qubole_run_hive_query]

    # [START howto_operator_qubole_run_hive_script]
    hive_s3_location = QuboleOperator(
        task_id='hive_s3_location',
        command_type="hivecmd",
        script_location="s3n://public-qubole/qbol-library/scripts/show_table.hql",
        notify=True,
        tags=['tag1', 'tag2'],
        # If the script at s3 location has any qubole specific macros to be replaced
        # macros='[{"date": "{{ ds }}"}, {"name" : "abc"}]',
    )
    # [END howto_operator_qubole_run_hive_script]

    options = ['hadoop_jar_cmd', 'presto_cmd', 'db_query', 'spark_cmd']

    branching = BranchPythonOperator(task_id='branching', python_callable=lambda: random.choice(options))

    [hive_show_table, hive_s3_location] >> compare_result(hive_s3_location, hive_show_table) >> branching

    join = EmptyOperator(task_id='join', trigger_rule=TriggerRule.ONE_SUCCESS)

    # [START howto_operator_qubole_run_hadoop_jar]
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
    # [END howto_operator_qubole_run_hadoop_jar]

    # [START howto_operator_qubole_run_pig_script]
    pig_cmd = QuboleOperator(
        task_id='pig_cmd',
        command_type="pigcmd",
        script_location="s3://public-qubole/qbol-library/scripts/script1-hadoop-s3-small.pig",
        parameters="key1=value1 key2=value2",
    )
    # [END howto_operator_qubole_run_pig_script]

    branching >> hadoop_jar_cmd >> pig_cmd >> join

    # [START howto_operator_qubole_run_presto_query]
    presto_cmd = QuboleOperator(task_id='presto_cmd', command_type='prestocmd', query='show tables')
    # [END howto_operator_qubole_run_presto_query]

    # [START howto_operator_qubole_run_shell_script]
    shell_cmd = QuboleOperator(
        task_id='shell_cmd',
        command_type="shellcmd",
        script_location="s3://public-qubole/qbol-library/scripts/shellx.sh",
        parameters="param1 param2",
    )
    # [END howto_operator_qubole_run_shell_script]

    branching >> presto_cmd >> shell_cmd >> join

    # [START howto_operator_qubole_run_db_tap_query]
    db_query = QuboleOperator(
        task_id='db_query', command_type='dbtapquerycmd', query='show tables', db_tap_id=2064
    )
    # [END howto_operator_qubole_run_db_tap_query]

    # [START howto_operator_qubole_run_db_export]
    db_export = QuboleOperator(
        task_id='db_export',
        command_type='dbexportcmd',
        mode=1,
        hive_table='default_qubole_airline_origin_destination',
        db_table='exported_airline_origin_destination',
        partition_spec='dt=20110104-02',
        dbtap_id=2064,
    )
    # [END howto_operator_qubole_run_db_export]

    branching >> db_query >> db_export >> join

    # [START howto_operator_qubole_run_db_import]
    db_import = QuboleOperator(
        task_id='db_import',
        command_type='dbimportcmd',
        mode=1,
        hive_table='default_qubole_airline_origin_destination',
        db_table='exported_airline_origin_destination',
        where_clause='id < 10',
        parallelism=2,
        dbtap_id=2064,
    )
    # [END howto_operator_qubole_run_db_import]

    # [START howto_operator_qubole_run_spark_scala]
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
    # [END howto_operator_qubole_run_spark_scala]

    branching >> db_import >> spark_cmd >> join

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
