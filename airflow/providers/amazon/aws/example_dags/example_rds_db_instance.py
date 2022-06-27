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
    RdsCreateDbInstanceOperator,
    RdsDeleteDbInstanceOperator,
)

RDS_DB_IDENTIFIER = getenv("RDS_DB_IDENTIFIER", "database-identifier")

with DAG(
    dag_id='example_rds_instance',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_rds_create_db_instance]
    create_db_instance = RdsCreateDbInstanceOperator(
        task_id='create_db_instance',
        db_name=RDS_DB_IDENTIFIER,
        db_instance_identifier=RDS_DB_IDENTIFIER,
        db_instance_class="db.m5.large",
        engine="postgres",
    )
    # [END howto_operator_rds_create_db_instance]

    # [START howto_operator_rds_delete_db_instance]
    delete_db_instance = RdsDeleteDbInstanceOperator(
        task_id='delete_db_instance',
        db_instance_identifier=RDS_DB_IDENTIFIER,
    )
    # [END howto_operator_rds_delete_db_instance]

    chain(create_db_instance, delete_db_instance)
