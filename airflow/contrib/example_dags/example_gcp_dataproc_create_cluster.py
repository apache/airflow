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


import airflow
from airflow import models
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

cluster_name = "testcluster-800"
project_id = ""
region = ""
zone = ""

# When using autoscaling the number of primary workers
# must be within autoscaler min/max range
num_workers = 2

# Autoscaling policy
# The policy must be in the same project and Cloud Dataproc region
# but the zone doesn't have to be the same
scaling_policy = 'test-policy'
policy_uri = 'projects/{p}/locations/{r}/autoscalingPolicies/{id}'.format(
    p=project_id,
    r=region,
    id=scaling_policy
)

with models.DAG(
    "example_dataproc_create_cluster",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    create_task = DataprocClusterCreateOperator(
        task_id="run_example_cluster_script",
        cluster_name=cluster_name,
        project_id=project_id,
        num_workers=num_workers,
        region=region,
        zone=zone,
        autoscaling_policy=policy_uri
    )
