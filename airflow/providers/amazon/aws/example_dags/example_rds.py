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

import os

from airflow import models
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.rds import RdsDescribeDbInstanceSnapshotsOperator, RdsRestoreDbInstanceFromSnapshotOperator, RdsDescribeDbInstancesOperator, RdsDeleteDbInstanceOperator, NewRdsBaseOperator
from airflow.providers.amazon.aws.sensors.rds import RdsDbSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import datetime

AWS_CONNECTION_ID = "aws_default"

@task
def get_rds_instance_identifier():
    return f"example-rds-{datetime.now():%Y-%m-%d-%H-%M-%S%z}"


class MyRebootRdsOperator(NewRdsBaseOperator):
    rds_client_callable: str = "reboot_db_instance"


with models.DAG(
    "example_rds",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    
    find_latest_production_snapshot = RdsDescribeDbInstanceSnapshotsOperator(
        task_id='find_latest_production_snapshot',
        aws_conn_id=AWS_CONNECTION_ID,
        rds_client_kwargs={
            "DBInstanceIdentifier": "production-database-name",
        },
        # filters out latest snapshot, returns its ARN
        result_handler=lambda results: sorted(results["DBSnapshots"], key=lambda x: x["SnapshotCreateTime"], reverse=True)[0]["DBSnapshotArn"]
    )

    rds_instance_identifier = get_rds_instance_identifier()

    restoure_from_snapshot = RdsRestoreDbInstanceFromSnapshotOperator(
        task_id='restoure_from_snapshot',
        aws_conn_id=AWS_CONNECTION_ID,
        rds_client_kwargs={
            "DBInstanceIdentifier": rds_instance_identifier,
            "DBSnapshotIdentifier": find_latest_production_snapshot.output,
            "DBInstanceClass": "db.t3.small",
            "MultiAZ": False,
            "PubliclyAccessible": False,
            "Tags": [
                {"Key": "foo", "Value": "bar"},
            ],
            "VpcSecurityGroupIds": ["sg-id-goes-here"],
            "CopyTagsToSnapshot": True,
            "EnableIAMDatabaseAuthentication": True,
            "DBParameterGroupName": "pg14.default",
            "DeletionProtection": False,
        },
        result_handler=lambda x: None
    )

    wait_until_rds_instance_available = RdsDbSensor(
        task_id='wait_until_rds_instance_available',
        aws_conn_id=AWS_CONNECTION_ID,
        db_type="instance",
        db_identifier=rds_instance_identifier,
        target_statuses=["available"],
        mode="reschedule",
        poke_interval=120,
    )

    get_temp_database_hostname = RdsDescribeDbInstancesOperator(
        task_id='get_temp_database_hostname',
        aws_conn_id=AWS_CONNECTION_ID,
        rds_client_kwargs={
            "DBInstanceIdentifier": rds_instance_identifier
        },
        result_handler=lambda result: result["DBInstances"][0]["Endpoint"]["Address"]
    )

    process_data = SQLExecuteQueryOperator(
        sql="SELECT * FROM table;"
    )

    reboot = MyRebootRdsOperator(
        aws_conn_id=AWS_CONNECTION_ID,
        rds_client_kwargs={
            "DBInstanceIdentifier": rds_instance_identifier
        }
    )

    destroy_db_instance = RdsDeleteDbInstanceOperator(
        task_id="destroy_db_instance",
        aws_conn_id=AWS_CONNECTION_ID,
        rds_client_kwargs={
            "DBInstanceIdentifier": get_rds_instance_identifier,
            "SkipFinalSnapshot": True
        }
    )

    (
        find_latest_production_snapshot
        >> rds_instance_identifier
        >> restoure_from_snapshot
        >> wait_until_rds_instance_available
        >> get_temp_database_hostname
        >> process_data
        >> reboot
        >> destroy_db_instance
    )
