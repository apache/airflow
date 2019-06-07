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
Example Airflow DAG for Google Dataproc PigOperator
"""

import os
import airflow
from airflow import models
from airflow.contrib.operators.dataproc_operator import (
    DataProcPigOperator,
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator
)

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

CLUSTER_NAME = os.environ.get('GCP_DATAPROC_CLUSTER_NAME', 'example-project')
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'an-id')
REGION = os.environ.get('GCP_LOCATION', 'europe-west1')


with models.DAG(
    "example_gcp_dataproc_pig_operator",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    create_task = DataprocClusterCreateOperator(
        task_id="create_task",
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        num_workers=2
    )

    pig_taks = DataProcPigOperator(
        task_id="pig_task",
        query="define sin HiveUDF('sin');",
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    delete_task = DataprocClusterDeleteOperator(
        task_id="delete_task",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )

    create_task >> pig_taks >> delete_task
