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
Example Airflow DAG that creates DataProc cluster.
"""

import os
import airflow
from airflow import models
from airflow.gcp.operators.dataproc import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator
)

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'an-id')
CLUSTER_NAME = os.environ.get('GCP_DATAPROC_CLUSTER_NAME', 'example-project')
REGION = os.environ.get('GCP_LOCATION', 'europe-west1')
ZONE = os.environ.get('GCP_REGION', 'europe-west-1b')

with models.DAG(
    "example_gcp_dataproc_create_cluster",
    default_args={"start_date": airflow.utils.dates.days_ago(1)},
    schedule_interval=None,
) as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id="create_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        num_workers=2,
        region=REGION,
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )

    create_cluster >> delete_cluster  # pylint: disable=pointless-statement
