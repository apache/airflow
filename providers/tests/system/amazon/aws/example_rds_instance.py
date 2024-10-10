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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.rds import (
    RdsCreateDbInstanceOperator,
    RdsDeleteDbInstanceOperator,
    RdsStartDbOperator,
    RdsStopDbOperator,
)
from airflow.providers.amazon.aws.sensors.rds import RdsDbSensor
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_rds_instance"

RDS_USERNAME = "database_username"
# NEVER store your production password in plaintext in a DAG like this.
# Use Airflow Secrets or a secret manager for this in production.
RDS_PASSWORD = "database_password"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    rds_db_identifier = f"{test_context[ENV_ID_KEY]}-database"

    # [START howto_operator_rds_create_db_instance]
    create_db_instance = RdsCreateDbInstanceOperator(
        task_id="create_db_instance",
        db_instance_identifier=rds_db_identifier,
        db_instance_class="db.t4g.micro",
        engine="postgres",
        rds_kwargs={
            "MasterUsername": RDS_USERNAME,
            "MasterUserPassword": RDS_PASSWORD,
            "AllocatedStorage": 20,
            "PubliclyAccessible": False,
        },
    )
    # [END howto_operator_rds_create_db_instance]

    # RdsCreateDbInstanceOperator waits by default, setting as False to test the Sensor below.
    create_db_instance.wait_for_completion = False

    # [START howto_sensor_rds_instance]
    await_db_instance = RdsDbSensor(
        task_id="await_db_instance",
        db_identifier=rds_db_identifier,
    )
    # [END howto_sensor_rds_instance]

    # [START howto_operator_rds_stop_db]
    stop_db_instance = RdsStopDbOperator(
        task_id="stop_db_instance",
        db_identifier=rds_db_identifier,
    )
    # [END howto_operator_rds_stop_db]

    # [START howto_operator_rds_start_db]
    start_db_instance = RdsStartDbOperator(
        task_id="start_db_instance",
        db_identifier=rds_db_identifier,
    )
    # [END howto_operator_rds_start_db]

    # [START howto_operator_rds_delete_db_instance]
    delete_db_instance = RdsDeleteDbInstanceOperator(
        task_id="delete_db_instance",
        db_instance_identifier=rds_db_identifier,
        rds_kwargs={
            "SkipFinalSnapshot": True,
        },
    )
    # [END howto_operator_rds_delete_db_instance]
    delete_db_instance.trigger_rule = TriggerRule.ALL_DONE

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        create_db_instance,
        await_db_instance,
        stop_db_instance,
        start_db_instance,
        delete_db_instance,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
