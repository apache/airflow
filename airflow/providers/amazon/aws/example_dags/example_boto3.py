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


AWS_CONNECTION_ID = "aws_default_conn"


@task
def generate_db_instance_identifier(**kwargs):
    return f"example-database-{datetime.now():%Y-%m-%d-%H-%M-%S%z}"


with DAG(
    dag_id="example-boto3-dag",
) as dag:
    """Example for boto3 DAG, in the case it interacts with RDS only. Can be used for any client that boto3 supports.
    What it does:
    - Looks for latest database instance snapshot
    - Restores it into new DB instance
    - Does some data manipulation (Not implemented in the example)
    - Destroys created DB instance
    """

    latest_snapshot_arn = Boto3Operator(
        task_id="get_latest_snapshot",
        aws_conn_id=AWS_CONNECTION_ID,
        boto3_callable="rds.describe_db_snapshots",
        boto3_kwargs={
            "DBInstanceIdentifier": "my-database",
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
        boto3_callable="rds.restore_db_instance_from_db_snapshot",
        boto3_kwargs={
            "DBInstanceIdentifier": db_instance_identifier,
            "DBSnapshotIdentifier": latest_snapshot_arn.output,
            "DBInstanceClass": "db.t2.small",
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
        boto3_callable="rds.describe_db_instances",
        boto3_kwargs={"DBInstanceIdentifier": db_instance_identifier},
        # Lambda function that returns True once criteria met
        poke_handler=lambda x: x["DBInstances"][0]["DBInstanceStatus"] == "available",
        mode="reschedule",
        poke_interval=300,
    )

    get_rds_endpoint_hostname = Boto3Operator(
        task_id="get_rds_endpoint_hostname",
        aws_conn_id=AWS_CONNECTION_ID,
        boto3_callable="rds.describe_db_instances",
        boto3_kwargs={"DBInstanceIdentifier": db_instance_identifier},
        # Extract endpoint address that we can connect to
        result_handler=lambda result: result["DBInstances"][0]["Endpoint"]["Address"],
    )

    ############################################
    ### Data processing opearators goes here ###
    ############################################

    destroy_db_instance = Boto3Operator(
        task_id="destroy_db_instance",
        aws_conn_id=AWS_CONNECTION_ID,
        boto3_callable="rds.delete_db_instance",
        boto3_kwargs={
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
