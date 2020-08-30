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
Example Airflow DAG to submit Apache Spark applications using
`SparkSubmitOperator`, `SparkJDBCOperator` and `SparkSqlOperator`.
"""
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='example_spark_operator',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    # [START howto_operator_spark_submit]
    submit_job = SparkSubmitOperator(
        application="${SPARK_HOME}/examples/src/main/python/pi.py", task_id="submit_job"
    )
    # [END howto_operator_spark_submit]

    # [START howto_operator_spark_jdbc]
    jdbc_to_spark_job = SparkJDBCOperator(
        cmd_type='jdbc_to_spark',
        jdbc_table="foo",
        spark_jars="${SPARK_HOME}/jars/postgresql-42.2.12.jar",
        jdbc_driver="org.postgresql.Driver",
        metastore_table="bar",
        save_mode="overwrite",
        save_format="JSON",
        task_id="jdbc_to_spark_job",
    )

    spark_to_jdbc_job = SparkJDBCOperator(
        cmd_type='spark_to_jdbc',
        jdbc_table="foo",
        spark_jars="${SPARK_HOME}/jars/postgresql-42.2.12.jar",
        jdbc_driver="org.postgresql.Driver",
        metastore_table="bar",
        save_mode="append",
        task_id="spark_to_jdbc_job",
    )
    # [END howto_operator_spark_jdbc]

    # [START howto_operator_spark_sql]
    sql_job = SparkSqlOperator(sql="SELECT * FROM bar", master="local", task_id="sql_job")
    # [END howto_operator_spark_sql]
