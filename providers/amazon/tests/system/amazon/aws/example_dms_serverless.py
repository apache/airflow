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
https://docs.aws.amazon.com/dms/latest/userguide/security-iam.html#CHAP_Security.APIRole
"""

from __future__ import annotations

import json
from datetime import datetime

import boto3
from sqlalchemy import Column, MetaData, String, Table, create_engine

from airflow.providers.amazon.aws.operators.dms import (
    DmsCreateReplicationConfigOperator,
    DmsDeleteReplicationConfigOperator,
    DmsDescribeReplicationConfigsOperator,
    DmsDescribeReplicationsOperator,
)
from airflow.providers.amazon.aws.operators.rds import (
    RdsCreateDbInstanceOperator,
    RdsDeleteDbInstanceOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator

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

"""
This example demonstrates how to use the DMS operators to create a serverless replication task to replicate data
from a PostgreSQL database to Amazon S3.

The IAM role used for the replication must have the permissions defined in the [Amazon S3 target](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.Prerequisites)
documentation.
"""

DAG_ID = "example_dms_serverless"
ROLE_ARN_KEY = "ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

# Config values for setting up the "Source" database.
CA_CERT_ID = "rds-ca-rsa2048-g1"
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


def _get_rds_instance_endpoint(instance_name: str):
    print("Retrieving RDS instance endpoint.")
    rds_client = boto3.client("rds")

    response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_name)
    rds_instance_endpoint = response["DBInstances"][0]["Endpoint"]
    return rds_instance_endpoint


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

    with engine.connect() as connection:
        # Create the Table.
        table.create(bind=connection)
        load_data = table.insert().values(SAMPLE_DATA)
        connection.execute(load_data)

        # Read the data back to verify everything is working.
        connection.execute(table.select())


@task(multiple_outputs=True)
def create_dms_assets(
    db_name: str,
    instance_name: str,
    bucket_name: str,
    role_arn,
    source_endpoint_identifier: str,
    target_endpoint_identifier: str,
    table_definition: dict,
):
    print("Creating DMS assets.")
    dms_client = boto3.client("dms")
    rds_instance_endpoint = _get_rds_instance_endpoint(instance_name)

    print("Creating DMS source endpoint.")
    source_endpoint_arn = dms_client.create_endpoint(
        EndpointIdentifier=source_endpoint_identifier,
        EndpointType="source",
        EngineName=RDS_ENGINE,
        Username=RDS_USERNAME,
        Password=RDS_PASSWORD,
        ServerName=rds_instance_endpoint["Address"],
        Port=rds_instance_endpoint["Port"],
        DatabaseName=db_name,
        SslMode="require",
    )["Endpoint"]["EndpointArn"]

    print("Creating DMS target endpoint.")
    target_endpoint_arn = dms_client.create_endpoint(
        EndpointIdentifier=target_endpoint_identifier,
        EndpointType="target",
        EngineName="s3",
        S3Settings={
            "BucketName": bucket_name,
            "BucketFolder": "folder",
            "ServiceAccessRoleArn": role_arn,
            "ExternalTableDefinition": json.dumps(table_definition),
        },
    )["Endpoint"]["EndpointArn"]

    return {
        "source_endpoint_arn": source_endpoint_arn,
        "target_endpoint_arn": target_endpoint_arn,
    }


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_dms_assets(
    source_endpoint_arn: str,
    target_endpoint_arn: str,
    source_endpoint_identifier: str,
    target_endpoint_identifier: str,
):
    dms_client = boto3.client("dms")
    dms_client.delete_endpoint(EndpointArn=source_endpoint_arn)
    dms_client.delete_endpoint(EndpointArn=target_endpoint_arn)
    dms_client.get_waiter("endpoint_deleted").wait(
        Filters=[
            {
                "Name": "endpoint-id",
                "Values": [source_endpoint_identifier, target_endpoint_identifier],
            }
        ]
    )


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    role_arn = test_context[ROLE_ARN_KEY]

    bucket_name = f"{env_id}-dms-serverless-bucket"
    rds_instance_name = f"{env_id}-instance"
    rds_db_name = f"{env_id}_source_database"  # dashes are not allowed in db name
    rds_table_name = f"{env_id}-table"
    source_endpoint_identifier = f"{env_id}-source-endpoint"
    target_endpoint_identifier = f"{env_id}-target-endpoint"
    replication_id = f"{env_id}-replication-id"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=bucket_name)

    create_db_instance = RdsCreateDbInstanceOperator(
        task_id="create_db_instance",
        db_instance_identifier=rds_instance_name,
        db_instance_class="db.t3.micro",
        engine=RDS_ENGINE,
        rds_kwargs={
            "DBName": rds_db_name,
            "AllocatedStorage": 20,
            "MasterUsername": RDS_USERNAME,
            "MasterUserPassword": RDS_PASSWORD,
            "PubliclyAccessible": True,
        },
    )

    # Sample data.
    table_definition = {
        "TableCount": "1",
        "Tables": [
            {
                "TableName": rds_table_name,
                "TableColumns": [
                    {
                        "ColumnName": TABLE_HEADERS[0],
                        "ColumnType": "STRING",
                        "ColumnNullable": "false",
                        "ColumnIsPk": "true",
                    },
                    {"ColumnName": TABLE_HEADERS[1], "ColumnType": "STRING", "ColumnLength": "4"},
                ],
                "TableColumnsTotal": "2",
            }
        ],
    }
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

    create_assets = create_dms_assets(
        db_name=rds_db_name,
        instance_name=rds_instance_name,
        bucket_name=bucket_name,
        role_arn=role_arn,
        source_endpoint_identifier=source_endpoint_identifier,
        target_endpoint_identifier=target_endpoint_identifier,
        table_definition=table_definition,
    )

    # [START howto_operator_dms_create_replication_config]
    create_replication_config = DmsCreateReplicationConfigOperator(
        task_id="create_replication_config",
        replication_config_id=replication_id,
        source_endpoint_arn=create_assets["source_endpoint_arn"],
        target_endpoint_arn=create_assets["target_endpoint_arn"],
        compute_config={
            "MaxCapacityUnits": 4,
            "MinCapacityUnits": 1,
            "MultiAZ": False,
            "ReplicationSubnetGroupId": "default",
        },
        replication_type="full-load",
        table_mappings=json.dumps(table_mappings),
    )
    # [END howto_operator_dms_create_replication_config]

    # [START howto_operator_dms_describe_replication_config]
    describe_replication_configs = DmsDescribeReplicationConfigsOperator(
        task_id="describe_replication_configs",
    )
    # [END howto_operator_dms_describe_replication_config]

    # [START howto_operator_dms_serverless_describe_replication]
    describe_replications = DmsDescribeReplicationsOperator(
        task_id="describe_replications",
    )
    # [END howto_operator_dms_serverless_describe_replication]

    # Comment the next two tasks because they take too much time to be run in the CI
    # Keep them for documentation purposes
    """
    # [START howto_operator_dms_serverless_start_replication]
    replicate = DmsStartReplicationOperator(
        task_id="replicate",
        replication_config_arn="{{ task_instance.xcom_pull(task_ids='create_replication_config', key='return_value') }}",
        replication_start_type="start-replication",
        wait_for_completion=True,
        waiter_delay=60,
        waiter_max_attempts=200,
    )
    # [END howto_operator_dms_serverless_start_replication]

    # [START howto_operator_dms_serverless_stop_replication]
    stop_replication = DmsStopReplicationOperator(
        task_id="stop_replication",
        replication_config_arn="{{ task_instance.xcom_pull(task_ids='create_replication_config', key='return_value') }}",
        wait_for_completion=True,
        waiter_delay=120,
        waiter_max_attempts=200,
    )
    # [END howto_operator_dms_serverless_stop_replication]
    """

    # [START howto_operator_dms_serverless_delete_replication_config]
    delete_replication_config = DmsDeleteReplicationConfigOperator(
        task_id="delete_replication_config",
        waiter_max_attempts=200,
        replication_config_arn="{{ task_instance.xcom_pull(task_ids='create_replication_config', key='return_value') }}",
    )
    # [END howto_operator_dms_serverless_delete_replication_config]
    delete_replication_config.trigger_rule = TriggerRule.ALL_DONE

    delete_assets = delete_dms_assets(
        source_endpoint_arn=create_assets["source_endpoint_arn"],
        target_endpoint_arn=create_assets["target_endpoint_arn"],
        source_endpoint_identifier=source_endpoint_identifier,
        target_endpoint_identifier=target_endpoint_identifier,
    )

    delete_db_instance = RdsDeleteDbInstanceOperator(
        task_id="delete_db_instance",
        db_instance_identifier=rds_instance_name,
        rds_kwargs={
            "SkipFinalSnapshot": True,
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket,
        create_db_instance,
        create_sample_table(rds_instance_name, rds_db_name, rds_table_name),
        create_assets,
        # TEST BODY
        create_replication_config,
        describe_replication_configs,
        describe_replications,
        delete_replication_config,
        # TEST TEARDOWN
        delete_assets,
        delete_db_instance,
        delete_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
