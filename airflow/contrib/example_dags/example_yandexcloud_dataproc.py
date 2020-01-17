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

from airflow import DAG
from airflow.contrib.operators.yandexcloud_dataproc_operator import (
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocRunHiveJobOperator,
    DataprocRunMapReduceJobOperator, DataprocRunPysparkJobOperator, DataprocRunSparkJobOperator,
)
from airflow.utils.dates import days_ago

# should be filled with appropriate ids

# Airflow connection with type "yandexcloud" must be created.
# By default connection with id "yandexcloud_default" will be used
CONNECTION_ID = 'yandexcloud_default'

# Name of the datacenter where Dataproc cluster will be created
AVAILABILITY_ZONE_ID = 'ru-central1-c'

# Dataproc cluster jobs will produce logs in specified s3 bucket
S3_BUCKET_NAME_FOR_JOB_LOGS = ''


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    'example_yandexcloud_dataproc_operator',
    default_args=default_args,
    schedule_interval=None,
    tags=['example'],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        zone=AVAILABILITY_ZONE_ID,
        connection_id=CONNECTION_ID,
        s3_bucket=S3_BUCKET_NAME_FOR_JOB_LOGS,
    )

    run_hive_query = DataprocRunHiveJobOperator(
        task_id='run_hive_query',
        query='SELECT 1;',
    )

    run_hive_query_from_file = DataprocRunHiveJobOperator(
        task_id='run_hive_query_from_file',
        query_file_uri='s3a://data-proc-public/jobs/sources/hive-001/main.sql',
        script_variables={
            'CITIES_URI': 's3a://data-proc-public/jobs/sources/hive-001/cities/',
            'COUNTRY_CODE': 'RU',
        }
    )

    run_mapreduce_job = DataprocRunMapReduceJobOperator(
        task_id='run_mapreduce_job',
        main_class='org.apache.hadoop.streaming.HadoopStreaming',
        file_uris=[
            's3a://data-proc-public/jobs/sources/mapreduce-001/mapper.py',
            's3a://data-proc-public/jobs/sources/mapreduce-001/reducer.py'
        ],
        arguments=[
            '-mapper', 'mapper.py',
            '-reducer', 'reducer.py',
            '-numReduceTasks', '1',
            '-input', 's3a://data-proc-public/jobs/sources/data/cities500.txt.bz2',
            '-output', 's3a://{bucket}/dataproc/job/results'.format(bucket=S3_BUCKET_NAME_FOR_JOB_LOGS)
        ],
        properties={
            'yarn.app.mapreduce.am.resource.mb': '2048',
            'yarn.app.mapreduce.am.command-opts': '-Xmx2048m',
            'mapreduce.job.maps': '6',
        },
    )

    run_spark_job = DataprocRunSparkJobOperator(
        task_id='run_spark_job',
        main_jar_file_uri='s3a://data-proc-public/jobs/sources/java/dataproc-examples-1.0.jar',
        main_class='ru.yandex.cloud.dataproc.examples.PopulationSparkJob',
        file_uris=[
            's3a://data-proc-public/jobs/sources/data/config.json',
        ],
        archive_uris=[
            's3a://data-proc-public/jobs/sources/data/country-codes.csv.zip',
        ],
        jar_file_uris=[
            's3a://data-proc-public/jobs/sources/java/icu4j-61.1.jar',
            's3a://data-proc-public/jobs/sources/java/commons-lang-2.6.jar',
            's3a://data-proc-public/jobs/sources/java/opencsv-4.1.jar',
            's3a://data-proc-public/jobs/sources/java/json-20190722.jar'
        ],
        arguments=[
            's3a://data-proc-public/jobs/sources/data/cities500.txt.bz2',
            's3a://{bucket}/dataproc/job/results/${{JOB_ID}}'.format(bucket=S3_BUCKET_NAME_FOR_JOB_LOGS),
        ],
        properties={
            'spark.submit.deployMode': 'cluster',
        },
    )

    run_pyspark_job = DataprocRunPysparkJobOperator(
        task_id='run_pyspark_job',
        main_python_file_uri='s3a://data-proc-public/jobs/sources/pyspark-001/main.py',
        python_file_uris=[
            's3a://data-proc-public/jobs/sources/pyspark-001/geonames.py',
        ],
        file_uris=[
            's3a://data-proc-public/jobs/sources/data/config.json',
        ],
        archive_uris=[
            's3a://data-proc-public/jobs/sources/data/country-codes.csv.zip',
        ],
        arguments=[
            's3a://data-proc-public/jobs/sources/data/cities500.txt.bz2',
            's3a://{bucket}/jobs/results/${{JOB_ID}}'.format(bucket=S3_BUCKET_NAME_FOR_JOB_LOGS),
        ],
        jar_file_uris=[
            's3a://data-proc-public/jobs/sources/java/dataproc-examples-1.0.jar',
            's3a://data-proc-public/jobs/sources/java/icu4j-61.1.jar',
            's3a://data-proc-public/jobs/sources/java/commons-lang-2.6.jar',
        ],
        properties={
            'spark.submit.deployMode': 'cluster',
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
    )

    create_cluster >> run_mapreduce_job >> run_hive_query >> run_hive_query_from_file \
        >> run_spark_job >> run_pyspark_job >> delete_cluster
