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
* PROJECT_ID - Google Cloud Project where the Cloud Function exists.
* LOCATION - Google Cloud Functions region where the function exists.
* ENTRYPOINT - Name of the executable function in the source code.
"""

import os
import datetime

import airflow
from airflow import models
from airflow.contrib.operators.gcp_function_operator import GcfFunctionDeleteOperator

# [START howto_operator_gcf_delete_args]
PROJECT_ID = os.environ.get('PROJECT_ID', 'example-project')
LOCATION = os.environ.get('LOCATION', 'europe-west1')
ENTRYPOINT = os.environ.get('ENTRYPOINT', 'helloWorld')
# A fully-qualified name of the function to delete

FUNCTION_NAME = 'projects/{}/locations/{}/functions/{}'.format(PROJECT_ID, LOCATION,
                                                               ENTRYPOINT)
# [END howto_operator_gcf_delete_args]

default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}

with models.DAG(
    'example_gcp_function_delete',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    # [START howto_operator_gcf_delete]
    t1 = GcfFunctionDeleteOperator(
        task_id="gcf_delete_task",
        name=FUNCTION_NAME
    )
    # [END howto_operator_gcf_delete]
