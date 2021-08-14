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

from airflow.models.dag import DAG
from airflow.providers.alibaba.cloud.operators.oss import OSSCreateBucketOperator, OSSDeleteBucketOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='oss_bucket_dag',
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:

    # [START howto_operator_oss_bucket]
    create_bucket = OSSCreateBucketOperator(
        oss_conn_id='oss_default', region='your region', task_id='task1', bucket_name='your bucket'
    )

    delete_bucket = OSSDeleteBucketOperator(
        oss_conn_id='oss_default', region='your region', task_id='task2', bucket_name='your bucket'
    )
    # [END howto_operator_oss_bucket]

    create_bucket >> delete_bucket
