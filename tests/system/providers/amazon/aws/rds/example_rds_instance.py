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
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.rds import (
    RdsCreateDbInstanceOperator,
    RdsDeleteDbInstanceOperator,
)
from airflow.providers.amazon.aws.sensors.rds import RdsDbSensor
from tests.system.providers.amazon.aws.utils import set_env_id

ENV_ID = set_env_id()
DAG_ID = "example_rds_instance"

RDS_DB_IDENTIFIER = f'{ENV_ID}-database'
RDS_USERNAME = 'database_username'
# NEVER store your production password in plaintext in a DAG like this.
# Use Airflow Secrets or a secret manager for this in production.
RDS_PASSWORD = 'database_password'

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_rds_create_db_instance]
    create_db_instance = RdsCreateDbInstanceOperator(
        task_id='create_db_instance',
        db_instance_identifier=RDS_DB_IDENTIFIER,
        db_instance_class="db.t4g.micro",
        engine="postgres",
        rds_kwargs={
            "MasterUsername": RDS_USERNAME,
            "MasterUserPassword": RDS_PASSWORD,
            "AllocatedStorage": 20,
        },
    )
    # [END howto_operator_rds_create_db_instance]

    # [START howto_sensor_rds_instance]
    db_instance_available = RdsDbSensor(
        task_id="db_instance_available",
        db_identifier=RDS_DB_IDENTIFIER,
    )
    # [END howto_sensor_rds_instance]

    # [START howto_operator_rds_delete_db_instance]
    delete_db_instance = RdsDeleteDbInstanceOperator(
        task_id='delete_db_instance',
        db_instance_identifier=RDS_DB_IDENTIFIER,
        rds_kwargs={
            "SkipFinalSnapshot": True,
        },
    )
    # [END howto_operator_rds_delete_db_instance]

    chain(create_db_instance, db_instance_available, delete_db_instance)

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
