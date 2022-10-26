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

import os
import uuid
from datetime import datetime

from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreateHiveJobOperator,
    DataprocCreateMapReduceJobOperator,
    DataprocCreatePysparkJobOperator,
    DataprocCreateSparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Name of the datacenter where Dataproc cluster will be created
from airflow.utils.trigger_rule import TriggerRule

# should be filled with appropriate ids


AVAILABILITY_ZONE_ID = "ru-central1-c"

# Dataproc cluster jobs will produce logs in specified s3 bucket
S3_BUCKET_NAME_FOR_JOB_LOGS = ""

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_yandexcloud_dataproc_operator"

with DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        zone=AVAILABILITY_ZONE_ID,
        s3_bucket=S3_BUCKET_NAME_FOR_JOB_LOGS,
        computenode_count=1,
        computenode_max_hosts_count=5,
    )

    create_hive_query = DataprocCreateHiveJobOperator(
        task_id="create_hive_query",
        query="SELECT 1;",
    )

    create_hive_query_from_file = DataprocCreateHiveJobOperator(
        task_id="create_hive_query_from_file",
        query_file_uri="s3a://data-proc-public/jobs/sources/hive-001/main.sql",
        script_variables={
            "CITIES_URI": "s3a://data-proc-public/jobs/sources/hive-001/cities/",
            "COUNTRY_CODE": "RU",
        },
    )

    create_mapreduce_job = DataprocCreateMapReduceJobOperator(
        task_id="create_mapreduce_job",
        main_class="org.apache.hadoop.streaming.HadoopStreaming",
        file_uris=[
            "s3a://data-proc-public/jobs/sources/mapreduce-001/mapper.py",
            "s3a://data-proc-public/jobs/sources/mapreduce-001/reducer.py",
        ],
        args=[
            "-mapper",
            "mapper.py",
            "-reducer",
            "reducer.py",
            "-numReduceTasks",
            "1",
            "-input",
            "s3a://data-proc-public/jobs/sources/data/cities500.txt.bz2",
            "-output",
            f"s3a://{S3_BUCKET_NAME_FOR_JOB_LOGS}/dataproc/job/results/{uuid.uuid4()}",
        ],
        properties={
            "yarn.app.mapreduce.am.resource.mb": "2048",
            "yarn.app.mapreduce.am.command-opts": "-Xmx2048m",
            "mapreduce.job.maps": "6",
        },
    )

    create_spark_job = DataprocCreateSparkJobOperator(
        task_id="create_spark_job",
        main_jar_file_uri="s3a://data-proc-public/jobs/sources/java/dataproc-examples-1.0.jar",
        main_class="ru.yandex.cloud.dataproc.examples.PopulationSparkJob",
        file_uris=[
            "s3a://data-proc-public/jobs/sources/data/config.json",
        ],
        archive_uris=[
            "s3a://data-proc-public/jobs/sources/data/country-codes.csv.zip",
        ],
        jar_file_uris=[
            "s3a://data-proc-public/jobs/sources/java/icu4j-61.1.jar",
            "s3a://data-proc-public/jobs/sources/java/commons-lang-2.6.jar",
            "s3a://data-proc-public/jobs/sources/java/opencsv-4.1.jar",
            "s3a://data-proc-public/jobs/sources/java/json-20190722.jar",
        ],
        args=[
            "s3a://data-proc-public/jobs/sources/data/cities500.txt.bz2",
            f"s3a://{S3_BUCKET_NAME_FOR_JOB_LOGS}/dataproc/job/results/${{JOB_ID}}",
        ],
        properties={
            "spark.submit.deployMode": "cluster",
        },
        packages=["org.slf4j:slf4j-simple:1.7.30"],
        repositories=["https://repo1.maven.org/maven2"],
        exclude_packages=["com.amazonaws:amazon-kinesis-client"],
    )

    create_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id="create_pyspark_job",
        main_python_file_uri="s3a://data-proc-public/jobs/sources/pyspark-001/main.py",
        python_file_uris=[
            "s3a://data-proc-public/jobs/sources/pyspark-001/geonames.py",
        ],
        file_uris=[
            "s3a://data-proc-public/jobs/sources/data/config.json",
        ],
        archive_uris=[
            "s3a://data-proc-public/jobs/sources/data/country-codes.csv.zip",
        ],
        args=[
            "s3a://data-proc-public/jobs/sources/data/cities500.txt.bz2",
            f"s3a://{S3_BUCKET_NAME_FOR_JOB_LOGS}/dataproc/job/results/${{JOB_ID}}",
        ],
        jar_file_uris=[
            "s3a://data-proc-public/jobs/sources/java/dataproc-examples-1.0.jar",
            "s3a://data-proc-public/jobs/sources/java/icu4j-61.1.jar",
            "s3a://data-proc-public/jobs/sources/java/commons-lang-2.6.jar",
        ],
        properties={
            "spark.submit.deployMode": "cluster",
        },
        packages=["org.slf4j:slf4j-simple:1.7.30"],
        repositories=["https://repo1.maven.org/maven2"],
        exclude_packages=["com.amazonaws:amazon-kinesis-client"],
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", trigger_rule=TriggerRule.ALL_DONE
    )

    create_cluster >> create_mapreduce_job >> create_hive_query >> create_hive_query_from_file
    create_hive_query_from_file >> create_spark_job >> create_pyspark_job >> delete_cluster

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
