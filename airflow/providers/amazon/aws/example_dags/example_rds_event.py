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

from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.rds import (
    RdsCreateEventSubscriptionOperator,
    RdsDeleteEventSubscriptionOperator,
)

SUBSCRIPTION_NAME = getenv("SUBSCRIPTION_NAME", "subscription-name")
SNS_TOPIC_ARN = getenv("SNS_TOPIC_ARN", "arn:aws:sns:<region>:<account number>:MyTopic")
RDS_DB_IDENTIFIER = getenv("RDS_DB_IDENTIFIER", "database-identifier")

with DAG(
    dag_id='example_rds_event',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_rds_create_event_subscription]
    create_subscription = RdsCreateEventSubscriptionOperator(
        task_id='create_subscription',
        subscription_name=SUBSCRIPTION_NAME,
        sns_topic_arn=SNS_TOPIC_ARN,
        source_type='db-instance',
        source_ids=[RDS_DB_IDENTIFIER],
        event_categories=['availability'],
    )
    # [END howto_operator_rds_create_event_subscription]

    # [START howto_operator_rds_delete_event_subscription]
    delete_subscription = RdsDeleteEventSubscriptionOperator(
        task_id='delete_subscription',
        subscription_name=SUBSCRIPTION_NAME,
    )
    # [END howto_operator_rds_delete_event_subscription]

    chain(create_subscription, delete_subscription)
