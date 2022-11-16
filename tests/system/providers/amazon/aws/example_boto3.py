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

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.boto3 import Boto3Operator
from airflow.providers.amazon.aws.sensors.boto3 import Boto3Sensor
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder


sys_test_context_task = SystemTestContextBuilder().build()


AWS_CONNECTION_ID = "aws_default_conn"
DB_CLASS = "db.t3.small"

@task
def generate_db_instance_identifier(**kwargs):
    return f"example-database-{datetime.now():%Y-%m-%d-%H-%M-%S%z}"


with DAG(
    dag_id="example_boto3",
    tags=["example"],
    catchup=False,
    schedule="@once",
) as dag:
    """Example for boto3 DAG, in the case it interacts with RDS only. Can be used for any client that boto3 supports.
    What it does:
    - Looks for latest database instance snapshot
    - Restores it into new DB instance
    - Does some data manipulation (Not implemented in the example)
    - Destroys created DB instance
    """

    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]
    db_name = f"{env_id}-database"

    latest_snapshot_arn = Boto3Operator(
        task_id="get_latest_snapshot",
        aws_conn_id=AWS_CONNECTION_ID,
        client_type="rds",
        client_method="describe_db_snapshots",
        method_kwargs={
            "DBInstanceIdentifier": db_name,
        },
        # The operator itself lists all the snapshots, `result_handler` takes ARN of last one by creation date.
        # The result will be saved as xcom and will be available for consequent tasks
        result_handler=lambda results: sorted(
            results["DBSnapshots"], key=lambda x: x["SnapshotCreateTime"], reverse=True
        )[0]["DBSnapshotArn"],
    )

    db_instance_identifier = generate_db_instance_identifier()

    restore_db_from_snapshot = Boto3Operator(
        task_id="restore_db_from_snapshot",
        aws_conn_id=AWS_CONNECTION_ID,
        client_type="rds",
        client_method="restore_db_instance_from_db_snapshot",
        method_kwargs={
            "DBInstanceIdentifier": db_instance_identifier,
            "DBSnapshotIdentifier": latest_snapshot_arn.output,
            "DBInstanceClass": DB_CLASS,
            "DBSubnetGroupName": "my-subnet-group",
            "VpcSecurityGroupIds": ["sg-my-security-group"],
            "Tags": [
                {"Key": "env", "Value": "example"},
            ],
        },
        # Do not store any results as xcom
        result_handler=lambda x: None,
    )

    wait_until_rds_instance_available = Boto3Sensor(
        task_id="wait_until_rds_instance_available",
        aws_conn_id=AWS_CONNECTION_ID,
        client_type="rds",
        client_method="describe_db_instances",
        method_kwargs={"DBInstanceIdentifier": db_instance_identifier},
        # Lambda function that returns True once criteria met
        poke_handler=lambda x: x["DBInstances"][0]["DBInstanceStatus"] == "available",
        mode="reschedule",
        poke_interval=300,
    )

    get_rds_endpoint_hostname = Boto3Operator(
        task_id="get_rds_endpoint_hostname",
        aws_conn_id=AWS_CONNECTION_ID,
        client_type="rds",
        client_method="describe_db_instances",
        method_kwargs={"DBInstanceIdentifier": db_instance_identifier},
        # Extract endpoint address that we can connect to
        result_handler=lambda result: result["DBInstances"][0]["Endpoint"]["Address"],
    )

    ############################################
    ### Data processing operators goes here ###
    ############################################

    destroy_db_instance = Boto3Operator(
        task_id="destroy_db_instance",
        aws_conn_id=AWS_CONNECTION_ID,
        client_type="rds",
        client_method="delete_db_instance",
        method_kwargs={
            "DBInstanceIdentifier": db_instance_identifier,
            "SkipFinalSnapshot": True,
        },
    )

    (
        latest_snapshot_arn
        >> db_instance_identifier
        >> restore_db_from_snapshot
        >> wait_until_rds_instance_available
        >> get_rds_endpoint_hostname
        >> destroy_db_instance
    )

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
