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
Note:  DMS requires you to configure specific IAM roles/permissions.  For more information, see
https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Security.html#CHAP_Security.APIRole
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import cast

import boto3
import pendulum
from sqlalchemy import Column, MetaData, String, Table, create_engine

from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.operators.dms import (
    DmsCreateTaskOperator,
    DmsDeleteTaskOperator,
    DmsDescribeTasksOperator,
    DmsModifyTaskOperator,
    DmsReloadTablesOperator,
    DmsStartTaskOperator,
    DmsStopTaskOperator,
)
from airflow.providers.amazon.aws.operators.rds import (
    RdsCreateDbInstanceOperator,
    RdsDeleteDbInstanceOperator,
)
from airflow.providers.amazon.aws.sensors.dms import DmsTaskBaseSensor, DmsTaskCompletedSensor
from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from system.amazon.aws.utils.ec2 import get_default_vpc_id

DAG_ID = "example_dms"

sys_test_context_task = SystemTestContextBuilder().build()

# Config values for setting up the RDS databases.
RDS_ENGINE = "postgres"
RDS_PROTOCOL = "postgresql"
RDS_USERNAME = "username"
# NEVER store your production password in plaintext in a DAG like this.
# Use Airflow Secrets or a secret manager for this in production.
RDS_PASSWORD = "rds_password"
TABLE_HEADERS = ["apache_project", "release_year"]
SAMPLE_DATA = [
    ("Airflow", "2015"),
    ("OpenOffice", "2012"),
    ("Subversion", "2000"),
    ("NiFi", "2006"),
]
SG_IP_PERMISSION = {
    "FromPort": 5432,
    "IpProtocol": "All",
    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
}


def _get_rds_instance_endpoint(instance_name: str):
    print("Retrieving RDS instance endpoint.")
    rds_client = boto3.client("rds")

    response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_name)
    rds_instance_endpoint = response["DBInstances"][0]["Endpoint"]
    return rds_instance_endpoint


@task
def create_security_group(security_group_name: str, vpc_id: str):
    client = boto3.client("ec2")
    security_group = client.create_security_group(
        GroupName=security_group_name,
        Description="Created for DMS system test",
        VpcId=vpc_id,
    )
    client.get_waiter("security_group_exists").wait(
        GroupIds=[security_group["GroupId"]],
    )
    client.authorize_security_group_ingress(
        GroupId=security_group["GroupId"],
        IpPermissions=[SG_IP_PERMISSION],
    )

    return security_group["GroupId"]


@task(multiple_outputs=True)
def create_db_parameter_group(parameter_group_name: str):
    rds_client = boto3.client("rds")
    engine = rds_client.describe_db_engine_versions(Engine=RDS_ENGINE, DefaultOnly=True)["DBEngineVersions"][
        0
    ]

    rds_client.create_db_parameter_group(
        DBParameterGroupName=parameter_group_name,
        DBParameterGroupFamily=engine["DBParameterGroupFamily"],
        Description="Created for DMS system test logical replication",
    )
    rds_client.modify_db_parameter_group(
        DBParameterGroupName=parameter_group_name,
        Parameters=[
            {
                "ParameterName": "rds.logical_replication",
                "ParameterValue": "1",
                "ApplyMethod": "pending-reboot",
            }
        ],
    )
    return {
        "name": parameter_group_name,
        "engine_version": engine["EngineVersion"],
        "available_at": pendulum.now("UTC").add(minutes=5).isoformat(),
    }


@task
def create_sample_table(instance_name: str, db_name: str, table_name: str):
    print("Creating sample table.")

    rds_endpoint = _get_rds_instance_endpoint(instance_name)
    hostname = rds_endpoint["Address"]
    port = rds_endpoint["Port"]
    rds_url = f"{RDS_PROTOCOL}://{RDS_USERNAME}:{RDS_PASSWORD}@{hostname}:{port}/{db_name}"
    engine = create_engine(rds_url)

    table = Table(
        table_name,
        MetaData(),
        Column(TABLE_HEADERS[0], String, primary_key=True),
        Column(TABLE_HEADERS[1], String),
    )

    with engine.begin() as connection:
        # Create the Table.
        table.create(bind=connection)
        load_data = table.insert().values(SAMPLE_DATA)
        connection.execute(load_data)

        # Read the data back to verify everything is working.
        connection.execute(table.select())


@task
def create_target_database(instance_name: str, source_db_name: str, target_db_name: str):
    print("Creating target database.")

    rds_endpoint = _get_rds_instance_endpoint(instance_name)
    hostname = rds_endpoint["Address"]
    port = rds_endpoint["Port"]
    rds_url = f"{RDS_PROTOCOL}://{RDS_USERNAME}:{RDS_PASSWORD}@{hostname}:{port}/{source_db_name}"
    engine = create_engine(rds_url)
    quoted_target_db_name = engine.dialect.identifier_preparer.quote(target_db_name)

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.exec_driver_sql(f"CREATE DATABASE {quoted_target_db_name}")


@task
def await_table_load(replication_task_arn: str, schema_name: str, table_name: str):
    DmsHook().get_waiter("table_reload_complete").wait(
        ReplicationTaskArn=replication_task_arn,
        Filters=[
            {"Name": "schema-name", "Values": [schema_name]},
            {"Name": "table-name", "Values": [table_name]},
        ],
        WaiterConfig={"Delay": 10, "MaxAttempts": 60},
    )


@task(multiple_outputs=True)
def create_dms_assets(
    source_db_name: str,
    target_db_name: str,
    instance_name: str,
    replication_instance_name: str,
    source_endpoint_identifier: str,
    target_endpoint_identifier: str,
):
    print("Creating DMS assets.")
    dms_client = boto3.client("dms")
    rds_instance_endpoint = _get_rds_instance_endpoint(instance_name)

    print("Creating replication instance.")
    instance_arn = dms_client.create_replication_instance(
        ReplicationInstanceIdentifier=replication_instance_name,
        ReplicationInstanceClass="dms.t3.small",
    )["ReplicationInstance"]["ReplicationInstanceArn"]

    print("Creating DMS source endpoint.")
    source_endpoint_arn = dms_client.create_endpoint(
        EndpointIdentifier=source_endpoint_identifier,
        EndpointType="source",
        EngineName=RDS_ENGINE,
        Username=RDS_USERNAME,
        Password=RDS_PASSWORD,
        ServerName=rds_instance_endpoint["Address"],
        Port=rds_instance_endpoint["Port"],
        DatabaseName=source_db_name,
        SslMode="require",
    )["Endpoint"]["EndpointArn"]

    print("Creating DMS target endpoint.")
    target_endpoint_arn = dms_client.create_endpoint(
        EndpointIdentifier=target_endpoint_identifier,
        EndpointType="target",
        EngineName=RDS_ENGINE,
        Username=RDS_USERNAME,
        Password=RDS_PASSWORD,
        ServerName=rds_instance_endpoint["Address"],
        Port=rds_instance_endpoint["Port"],
        DatabaseName=target_db_name,
        SslMode="require",
    )["Endpoint"]["EndpointArn"]

    print("Awaiting replication instance provisioning.")
    dms_client.get_waiter("replication_instance_available").wait(
        Filters=[{"Name": "replication-instance-arn", "Values": [instance_arn]}]
    )

    return {
        "replication_instance_arn": instance_arn,
        "source_endpoint_arn": source_endpoint_arn,
        "target_endpoint_arn": target_endpoint_arn,
    }


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_dms_assets(
    replication_instance_arn: str,
    source_endpoint_arn: str,
    target_endpoint_arn: str,
    source_endpoint_identifier: str,
    target_endpoint_identifier: str,
    replication_instance_name: str,
):
    dms_client = boto3.client("dms")

    print("Deleting DMS assets.")
    dms_client.delete_replication_instance(ReplicationInstanceArn=replication_instance_arn)
    dms_client.delete_endpoint(EndpointArn=source_endpoint_arn)
    dms_client.delete_endpoint(EndpointArn=target_endpoint_arn)

    print("Awaiting DMS assets tear-down.")
    dms_client.get_waiter("replication_instance_deleted").wait(
        Filters=[{"Name": "replication-instance-id", "Values": [replication_instance_name]}]
    )
    dms_client.get_waiter("endpoint_deleted").wait(
        Filters=[
            {
                "Name": "endpoint-id",
                "Values": [source_endpoint_identifier, target_endpoint_identifier],
            }
        ]
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_security_group(security_group_id: str, security_group_name: str):
    boto3.client("ec2").delete_security_group(GroupId=security_group_id, GroupName=security_group_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_db_parameter_group(parameter_group_name: str):
    rds_client = boto3.client("rds")
    try:
        rds_client.delete_db_parameter_group(DBParameterGroupName=parameter_group_name)
    except rds_client.exceptions.DBParameterGroupNotFoundFault:
        print(f"DB parameter group {parameter_group_name} is already deleted.")


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    rds_instance_name = f"{env_id}-instance"
    rds_source_db_name = f"{env_id}_source_database"  # dashes are not allowed in db name
    rds_target_db_name = f"{env_id}_target_database"
    rds_table_name = f"{env_id}-table"
    dms_replication_instance_name = f"{env_id}-replication-instance"
    dms_replication_task_id = f"{env_id}-replication-task"
    source_endpoint_identifier = f"{env_id}-source-endpoint"
    target_endpoint_identifier = f"{env_id}-target-endpoint"
    security_group_name = f"{env_id}-dms-security-group"
    db_parameter_group_name = f"{env_id}-dms-parameter-group"
    db_parameter_group = create_db_parameter_group(db_parameter_group_name)
    await_db_parameter_group = DateTimeSensorAsync(
        task_id="await_db_parameter_group",
        target_time=db_parameter_group["available_at"],
    )

    table_mappings = {
        "rules": [
            {
                "rule-type": "selection",
                "rule-id": "1",
                "rule-name": "1",
                "object-locator": {
                    "schema-name": "public",
                    "table-name": rds_table_name,
                },
                "rule-action": "include",
            }
        ]
    }

    get_vpc_id = get_default_vpc_id()

    create_sg = create_security_group(security_group_name, get_vpc_id)

    create_db_instance = RdsCreateDbInstanceOperator(
        task_id="create_db_instance",
        db_instance_identifier=rds_instance_name,
        db_instance_class="db.t3.micro",
        engine=RDS_ENGINE,
        rds_kwargs={
            "DBName": rds_source_db_name,
            "AllocatedStorage": 20,
            "MasterUsername": RDS_USERNAME,
            "MasterUserPassword": RDS_PASSWORD,
            "PubliclyAccessible": True,
            "EngineVersion": db_parameter_group["engine_version"],
            "DBParameterGroupName": db_parameter_group["name"],
            "VpcSecurityGroupIds": [
                create_sg,
            ],
        },
    )

    create_target_db = create_target_database(
        instance_name=rds_instance_name,
        source_db_name=rds_source_db_name,
        target_db_name=rds_target_db_name,
    )

    create_assets = create_dms_assets(
        source_db_name=rds_source_db_name,
        target_db_name=rds_target_db_name,
        instance_name=rds_instance_name,
        replication_instance_name=dms_replication_instance_name,
        source_endpoint_identifier=source_endpoint_identifier,
        target_endpoint_identifier=target_endpoint_identifier,
    )

    # [START howto_operator_dms_create_task]
    create_task = DmsCreateTaskOperator(
        task_id="create_task",
        replication_task_id=dms_replication_task_id,
        source_endpoint_arn=create_assets["source_endpoint_arn"],
        target_endpoint_arn=create_assets["target_endpoint_arn"],
        replication_instance_arn=create_assets["replication_instance_arn"],
        table_mappings=table_mappings,
        migration_type="full-load-and-cdc",
        create_task_kwargs={
            "ReplicationTaskSettings": json.dumps({"ValidationSettings": {"EnableValidation": True}})
        },
    )
    # [END howto_operator_dms_create_task]

    task_arn = cast("str", create_task.output)

    # [START howto_operator_dms_start_task]
    start_task = DmsStartTaskOperator(
        task_id="start_task",
        replication_task_arn=task_arn,
    )
    # [END howto_operator_dms_start_task]

    # [START howto_operator_dms_describe_tasks]
    describe_tasks = DmsDescribeTasksOperator(
        task_id="describe_tasks",
        describe_tasks_kwargs={
            "Filters": [
                {
                    "Name": "replication-instance-arn",
                    "Values": [create_assets["replication_instance_arn"]],
                }
            ]
        },
        do_xcom_push=False,
    )
    # [END howto_operator_dms_describe_tasks]

    await_task_start = DmsTaskBaseSensor(
        task_id="await_task_start",
        replication_task_arn=task_arn,
        target_statuses=["running"],
        termination_statuses=["stopped", "deleting", "failed"],
        poke_interval=10,
    )

    await_initial_table_load = await_table_load(task_arn, "public", rds_table_name)

    # [START howto_operator_dms_reload_tables]
    reload_tables = DmsReloadTablesOperator(
        task_id="reload_tables",
        replication_task_arn=task_arn,
        tables_to_reload=[{"SchemaName": "public", "TableName": rds_table_name}],
        reload_option="data-reload",
        wait_for_completion=True,
        deferrable=True,
    )
    revalidate_tables = DmsReloadTablesOperator(
        task_id="revalidate_tables",
        replication_task_arn=task_arn,
        tables_to_reload=[{"SchemaName": "public", "TableName": rds_table_name}],
        reload_option="validate-only",
        wait_for_completion=True,
        deferrable=True,
    )
    # [END howto_operator_dms_reload_tables]

    # [START howto_operator_dms_stop_task]
    stop_task = DmsStopTaskOperator(
        task_id="stop_task",
        replication_task_arn=task_arn,
    )
    # [END howto_operator_dms_stop_task]

    # [START howto_operator_dms_modify_task]
    modify_task = DmsModifyTaskOperator(
        task_id="modify_task",
        replication_task_arn=task_arn,
        table_mappings={
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "1",
                    "rule-name": "1",
                    "object-locator": {"schema-name": "%", "table-name": "%"},
                    "rule-action": "include",
                }
            ]
        },
    )
    # [END howto_operator_dms_modify_task]

    # TaskCompletedSensor actually waits until task reaches the "Stopped" state, so it will work here.
    # [START howto_sensor_dms_task_completed]
    await_task_stop = DmsTaskCompletedSensor(
        task_id="await_task_stop",
        replication_task_arn=task_arn,
    )
    # [END howto_sensor_dms_task_completed]
    await_task_stop.poke_interval = 10

    # [START howto_operator_dms_delete_task]
    delete_task = DmsDeleteTaskOperator(
        task_id="delete_task",
        replication_task_arn=task_arn,
    )
    # [END howto_operator_dms_delete_task]
    delete_task.trigger_rule = TriggerRule.ALL_DONE

    delete_assets = delete_dms_assets(
        replication_instance_arn=create_assets["replication_instance_arn"],
        source_endpoint_arn=create_assets["source_endpoint_arn"],
        target_endpoint_arn=create_assets["target_endpoint_arn"],
        source_endpoint_identifier=source_endpoint_identifier,
        target_endpoint_identifier=target_endpoint_identifier,
        replication_instance_name=dms_replication_instance_name,
    )

    delete_db_instance = RdsDeleteDbInstanceOperator(
        task_id="delete_db_instance",
        db_instance_identifier=rds_instance_name,
        rds_kwargs={
            "SkipFinalSnapshot": True,
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_parameter_group = delete_db_parameter_group(db_parameter_group_name)

    chain(
        # TEST SETUP
        test_context,
        get_vpc_id,
        create_sg,
        db_parameter_group,
        await_db_parameter_group,
        create_db_instance,
        create_target_db,
        create_sample_table(rds_instance_name, rds_source_db_name, rds_table_name),
        create_assets,
        # TEST BODY
        create_task,
        start_task,
        describe_tasks,
        await_task_start,
        await_initial_table_load,
        reload_tables,
        revalidate_tables,
        stop_task,
        await_task_stop,
        modify_task,
        # TEST TEARDOWN
        delete_task,
        delete_assets,
        delete_db_instance,
        delete_parameter_group,
        delete_security_group(create_sg, security_group_name),
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
