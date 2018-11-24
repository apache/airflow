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
# specific language governing permissions andf limitations
# under the License.

import airflow
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DAG
from datetime import timedelta

log = LoggingMixin().log

try:
    # AWS Batch is optional, so not available in vanilla Airflow
    # pip install apache-airflow[boto3]
    from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(2),
        'email': ['airflow@airflow.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    dag = DAG(
        'example_awsbatch_dag', default_args=default_args, schedule_interval=timedelta(1))

    # vanilla example
    t0 = AWSBatchOperator(
        task_id='airflow-vanilla',
        job_name='airflow-vanilla',
        job_queue='airflow',
        job_definition='airflow',
        overrides={},
        queue='airflow',
        dag=dag)

    # overrides example
    t1 = AWSBatchOperator(
        job_name='airflow-overrides',
        task_id='airflow-overrides',
        job_queue='airflow',
        job_definition='airflow',
        overrides={
            "command": [
                "echo",
                "overrides"
            ]
        },
        queue='airflow',
        dag=dag)

    # parameters example
    t2 = AWSBatchOperator(
        job_name='airflow-parameters',
        task_id='airflow-parameters',
        job_queue='airflow',
        job_definition='airflow',
        overrides={
            "command": [
                "echo",
                "Ref::input"
            ]
        },
        parameters={
            "input": "Airflow2000"
        },
        queue='airflow',
        dag=dag)

    t0.set_upstream(t1)
    t1.set_upstream(t2)

except ImportError as e:
    log.warn("Could not import AWSBatchOperator: " + str(e))
    log.warn("Install AWS Batch dependencies with: "
             "    pip install apache-airflow[boto3]")
