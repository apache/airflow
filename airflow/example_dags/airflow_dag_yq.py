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
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum
import dateutil

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.yandex.operators.yandexcloud_yq import YQExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import base64
# import airflow.providers.yandex.operators.yandexcloud_dataproc

with DAG(
    dag_id="yq_operator",
    # schedule="@daily",
    schedule_interval='30 2 * * *',
    start_date=pendulum.datetime(2023, 1, 16, 19, 15, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
) as dag:
    folder_id = "b1gaud5b392mmmeolb0k"
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # # [START howto_operator_bash]
    # run_this = BashOperator(
    #     task_id="run_after_loop",
    #     bash_command="echo 1",
    # )
    # # [END howto_operator_bash]

    # run_this >> run_this_last

    # # [END howto_operator_bash_template]
    # also_run_this >> run_this_last

        # @task
        # def yq_read_data():
        #     operator = YQExecuteQueryOperator(task_id="samplequery2", sql="select 22 as d, 33 as t")
        #     return operator.execute()

        # @task
        # def ydb_write_data(yq_data):
        #     ydb_bulk_insert = YDBBulkUpsertOperator(task_id="bulk_insert",
        #                                             endpoint="grpcs://ydb.serverless.yandexcloud.net:2135",
        #                                             database="/ru-central1/b1g8skpblkos03malf3s/etndta0jk4us20e557i7",
        #                                             table="my_table",
        #                                             column_types={"id": ydb.PrimitiveType.Uint64,
        #                                                     "name": ydb.PrimitiveType.Utf8},
        #                                             values=[{"id":1,"name":"v"}])
        #     return ydb_bulk_insert.execute()

        # data = yq_read_data()
        # ydb_write_data(data)

    def base64ToString(b):
        return base64.b64decode(b).decode('utf-8')

    def get_column_index(columns, name):
        for index, column in enumerate(columns):
            if column["name"] == name:
                return index

    def get_col_by_name(row, columns, name):
        index = get_column_index(columns, name)
        value = row[index]
        # print(value)
        return value

    def process_query_count_result(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='get_queries_count')
        # print(result)

        print(f"Incoming rows={result['rows']}")

    def process_result(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='samplequery2')

        print(f"Incoming rows={result['rows']}")


    # ydb_bulk_insert >> run_this_last

    query = """
$parse_ingress_bytes = ($m) -> {
    $t = "IngressBytes: [";
    $start = Find($m, $t);
    $end = Find($m, "]", $start);
    return Cast(Substring($m, $start+LEN($t), $end-$start-LEN($t)) as Uint64);
    };

$parse_folder_id = ($m) -> {
    $t = "scope: [yandexcloud://";
    $start = Find($m, $t);
    $end = Find($m, "]", $start);
    return Substring($m, $start+LEN($t), $end-$start-LEN($t));
    };

$parse_status = ($m) -> {
    $t = ", status:";
    $start = Find($m, $t);
    $end = LEN($m);
    return String::Strip(Substring($m, $start+LEN($t), $end-$start-LEN($t)));
    };

select * from (
select `@timestamp` as ts, $parse_ingress_bytes(message) as ingress_bytes, $parse_folder_id(message) as folder_id, $parse_status(message) as status from (select
   `yq_prod_logs_cold_projected`.*
FROM
   `yq_prod_logs_cold_projected`)
where component="YQ_AUDIT" and message like "FinalStatus%"
and message like "%IngressBytes%"
and `date` between Date("{{ data_interval_start | ds }}") and Date("{{ data_interval_end | ds }}")
)
where COALESCE(folder_id,"") != ""
limit 1000;

"""

    yq_operator2 = YQExecuteQueryOperator(task_id="samplequery2", sql=query, connection_id="yandexcloud_default", folder_id=folder_id)
    yq_operator2 >> run_this_last

    query_count_queries = """
$parse_folder_id = ($m) -> {
    $t = "scope: [yandexcloud://";
    $start = Find($m, $t);
    $end = Find($m, "]", $start);
    return Substring($m, $start+LEN($t), $end-$start-LEN($t));
    };

$parse_status = ($m) -> {
    $t = ", status:";
    $start = Find($m, $t);
    $end = LEN($m);
    return String::Strip(Substring($m, $start+LEN($t), $end-$start-LEN($t)));
    };

$parse_query_id = ($m) -> {
    $t = "query id: [";
    $start = Find($m, $t);
    $end = Find($m, "]", $start);
    return Substring($m, $start+LEN($t), $end-$start-LEN($t));
    };

select * from (
select `@timestamp` as ts, $parse_folder_id(message) as folder_id, $parse_status(message) as status, $parse_query_id(message) as query_id from (select
   `yq_prod_logs_cold_projected`.*
FROM
   `yq_prod_logs_cold_projected`)
where component="YQ_AUDIT" and message like "FinalStatus%"
and `date` between Date("{{ data_interval_start | ds }}") and Date("{{ data_interval_end | ds }}")
)
where COALESCE(folder_id,"") != ""
limit 1000;

"""

    process_query_count_task = PythonOperator(  task_id='process_query_count_result',
                                                python_callable=process_query_count_result,
                                                provide_context=True)

    yq_operator_queries_count = YQExecuteQueryOperator(task_id="get_queries_count", sql=query_count_queries, connection_id="yandexcloud_default", folder_id=folder_id)
    yq_operator_queries_count >> process_query_count_task

    # yq_operator3 = YQExecuteQueryOperator(task_id="samplequery3", sql="select 33 as d, 44 as t")
    # yq_operator3 >> run_this_last

    # yq_operator4 = YQExecuteQueryOperator(task_id="samplequery4", sql="select 33 as d, 44 as t")
    # # yq_operator4 >> ydb_bulk_insert


    # yq_operator = YQExecuteQueryOperator(task_id="samplequery", sql="select 1")
    # yq_operator >> yq_operator2


    process_result_task = PythonOperator(
            task_id='process_result',
            python_callable=process_result,
            provide_context=True)

    yq_operator2 >> process_result_task




    # # [START howto_operator_bash_skip]
    # this_will_skip = BashOperator(
    #     task_id="this_will_skip",
    #     bash_command='echo "hello world"; exit 99;',
    #     dag=dag,
    # )
    # # [END howto_operator_bash_skip]
    # this_will_skip >> run_this_last

if __name__ == "__main__":
    dag.test()
