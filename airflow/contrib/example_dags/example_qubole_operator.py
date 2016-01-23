from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator, BranchPythonOperator
from airflow.contrib.operators import QuboleOperator
from datetime import datetime, timedelta
import filecmp
import random

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG('example_qubole_operator', default_args=default_args)

def compare_result(ds, **kwargs):
    ti = kwargs['ti']
    r1 = t1.get_results(ti)
    r2 = t2.get_results(ti)
    return filecmp.cmp(r1, r2)

t1 = QuboleOperator(
    task_id='hive_show_table',
    command_type='hivecmd',
    query='show tables',
    cluster_label='default',
    fetch_logs=True,
    tags='aiflow_example_run',
    dag=dag)

t2 = QuboleOperator(
    task_id='hive_s3_location',
    command_type="hivecmd",
    script_location="s3n://dev.canopydata.com/airflow/show_table.hql",
    notfiy=True,
    tags='aiflow_example_run',
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
    sub_command='jar s3://paid-qubole/HadoopAPIExamples/jars/hadoop-0.20.1-dev-streaming.jar -mapper wc -numReduceTasks 0 -input s3://paid-qubole/HadoopAPITests/data/3.tsv -output s3://paid-qubole/HadoopAPITests/data/3_wc',
    cluster_label='default',
    fetch_logs=True,
    dag=dag)

t5 = QuboleOperator(
    task_id='pig_cmd',
    command_type="pigcmd",
    script_location="s3://paid-qubole/PigAPIDemo/scripts/script1-hadoop-s3-small.pig",
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
    script_location="s3://paid-qubole/ShellDemo/data/excite-small.sh",
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
    language='python',
    arguments='--class SparkPi',
    tags='aiflow_example_run',
    dag=dag)

t11.set_upstream(branching)
t11.set_downstream(t10)
t10.set_downstream(join)

