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
Example Airflow DAG that deletes a Google Cloud Function.
This DAG relies on the following OS environment variables
* GCP_PROJECT_ID - Google Cloud Project where the Cloud Function exists.
* GCP_LOCATION - Google Cloud Functions region where the function exists.
* GCF_ENTRYPOINT - Name of the executable function in the source code.
"""

import os
import datetime

import airflow
from airflow import models
from airflow.contrib.operators.gcp_function_operator import GcfFunctionDeleteOperator

# [START howto_operator_gcf_delete_args]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'europe-west1')
GCF_ENTRYPOINT = os.environ.get('GCF_ENTRYPOINT', 'helloWorld')
# A fully-qualified name of the function to delete

GCF_SHORT_FUNCTION_NAME = os.environ.get('GCF_SHORT_FUNCTION_NAME', 'hello')
FUNCTION_NAME = 'projects/{}/locations/{}/functions/{}'.format(GCP_PROJECT_ID,
                                                               GCP_LOCATION,
                                                               GCF_SHORT_FUNCTION_NAME)

# [END howto_operator_gcf_delete_args]

default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}

with models.DAG(
    'example_gcp_function_delete',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
    catchup=False
) as dag:
    # [START howto_operator_gcf_delete]
    t1 = GcfFunctionDeleteOperator(
        task_id="gcf_delete_task",
        name=FUNCTION_NAME
    )
    # [END howto_operator_gcf_delete]
