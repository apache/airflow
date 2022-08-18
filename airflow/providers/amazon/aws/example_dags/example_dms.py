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

import json
import os
from datetime import datetime

import boto3
from sqlalchemy import Column, MetaData, String, Table, create_engine

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.dms import (
    DmsCreateTaskOperator,
    DmsDeleteTaskOperator,
    DmsDescribeTasksOperator,
    DmsStartTaskOperator,
    DmsStopTaskOperator,
)
from airflow.providers.amazon.aws.operators.rds import (
    RdsCreateDbInstanceOperator,
    RdsDeleteDbInstanceOperator,
)
from airflow.providers.amazon.aws.sensors.dms import DmsTaskBaseSensor, DmsTaskCompletedSensor

S3_BUCKET = os.getenv('S3_BUCKET', 's3_bucket_name')
ROLE_ARN = os.getenv('ROLE_ARN', 'arn:aws:iam::1234567890:role/s3_target_endpoint_role')

# The project name will be used as a prefix for various entity names.
# Use either PascalCase or camelCase.  While some names require kebab-case
# and others require snake_case, they all accept mixedCase strings.
PROJECT_NAME = 'DmsDemo'

# Config values for setting up the "Source" database.
RDS_ENGINE = 'postgres'
RDS_PROTOCOL = 'postgresql'
RDS_USERNAME = 'username'
# NEVER store your production password in plaintext in a DAG like this.
# Use Airflow Secrets or a secret manager for this in production.
RDS_PASSWORD = 'rds_password'

# Config values for RDS.
RDS_INSTANCE_NAME = f'{PROJECT_NAME}-instance'
RDS_DB_NAME = f'{PROJECT_NAME}_source_database'

# Config values for DMS.
DMS_REPLICATION_INSTANCE_NAME = f'{PROJECT_NAME}-replication-instance'
DMS_REPLICATION_TASK_ID = f'{PROJECT_NAME}-replication-task'
SOURCE_ENDPOINT_IDENTIFIER = f'{PROJECT_NAME}-source-endpoint'
TARGET_ENDPOINT_IDENTIFIER = f'{PROJECT_NAME}-target-endpoint'

# Sample data.
TABLE_NAME = f'{PROJECT_NAME}-table'
TABLE_HEADERS = ['apache_project', 'release_year']
SAMPLE_DATA = [
    ('Airflow', '2015'),
    ('OpenOffice', '2012'),
    ('Subversion', '2000'),
    ('NiFi', '2006'),
]
TABLE_DEFINITION = {
    'TableCount': '1',
    'Tables': [
        {
            'TableName': TABLE_NAME,
            'TableColumns': [
                {
                    'ColumnName': TABLE_HEADERS[0],
                    'ColumnType': 'STRING',
                    'ColumnNullable': 'false',
                    'ColumnIsPk': 'true',
                },
                {"ColumnName": TABLE_HEADERS[1], "ColumnType": 'STRING', "ColumnLength": "4"},
            ],
            'TableColumnsTotal': '2',
        }
    ],
}
TABLE_MAPPINGS = {
    'rules': [
        {
            'rule-type': 'selection',
            'rule-id': '1',
            'rule-name': '1',
            'object-locator': {
                'schema-name': 'public',
                'table-name': TABLE_NAME,
            },
            'rule-action': 'include',
        }
    ]
}


def _get_rds_instance_endpoint():
    print('Retrieving RDS instance endpoint.')
    rds_client = boto3.client('rds')

    response = rds_client.describe_db_instances(DBInstanceIdentifier=RDS_INSTANCE_NAME)
    rds_instance_endpoint = response['DBInstances'][0]['Endpoint']
    return rds_instance_endpoint


@task
def create_sample_table():
    print('Creating sample table.')

    rds_endpoint = _get_rds_instance_endpoint()
    hostname = rds_endpoint['Address']
    port = rds_endpoint['Port']
    rds_url = f'{RDS_PROTOCOL}://{RDS_USERNAME}:{RDS_PASSWORD}@{hostname}:{port}/{RDS_DB_NAME}'
    engine = create_engine(rds_url)

    table = Table(
        TABLE_NAME,
        MetaData(engine),
        Column(TABLE_HEADERS[0], String, primary_key=True),
        Column(TABLE_HEADERS[1], String),
    )

    with engine.connect() as connection:
        # Create the Table.
        table.create()
        load_data = table.insert().values(SAMPLE_DATA)
        connection.execute(load_data)

        # Read the data back to verify everything is working.
        connection.execute(table.select())


@task
def create_dms_assets():
    print('Creating DMS assets.')
    ti = get_current_context()['ti']
    dms_client = boto3.client('dms')
    rds_instance_endpoint = _get_rds_instance_endpoint()

    print('Creating replication instance.')
    instance_arn = dms_client.create_replication_instance(
        ReplicationInstanceIdentifier=DMS_REPLICATION_INSTANCE_NAME,
        ReplicationInstanceClass='dms.t3.micro',
    )['ReplicationInstance']['ReplicationInstanceArn']

    ti.xcom_push(key='replication_instance_arn', value=instance_arn)

    print('Creating DMS source endpoint.')
    source_endpoint_arn = dms_client.create_endpoint(
        EndpointIdentifier=SOURCE_ENDPOINT_IDENTIFIER,
        EndpointType='source',
        EngineName=RDS_ENGINE,
        Username=RDS_USERNAME,
        Password=RDS_PASSWORD,
        ServerName=rds_instance_endpoint['Address'],
        Port=rds_instance_endpoint['Port'],
        DatabaseName=RDS_DB_NAME,
    )['Endpoint']['EndpointArn']

    print('Creating DMS target endpoint.')
    target_endpoint_arn = dms_client.create_endpoint(
        EndpointIdentifier=TARGET_ENDPOINT_IDENTIFIER,
        EndpointType='target',
        EngineName='s3',
        S3Settings={
            'BucketName': S3_BUCKET,
            'BucketFolder': PROJECT_NAME,
            'ServiceAccessRoleArn': ROLE_ARN,
            'ExternalTableDefinition': json.dumps(TABLE_DEFINITION),
        },
    )['Endpoint']['EndpointArn']

    ti.xcom_push(key='source_endpoint_arn', value=source_endpoint_arn)
    ti.xcom_push(key='target_endpoint_arn', value=target_endpoint_arn)

    print("Awaiting replication instance provisioning.")
    dms_client.get_waiter('replication_instance_available').wait(
        Filters=[{'Name': 'replication-instance-arn', 'Values': [instance_arn]}]
    )


@task(trigger_rule='all_done')
def delete_dms_assets():
    ti = get_current_context()['ti']
    dms_client = boto3.client('dms')
    replication_instance_arn = ti.xcom_pull(key='replication_instance_arn')
    source_arn = ti.xcom_pull(key='source_endpoint_arn')
    target_arn = ti.xcom_pull(key='target_endpoint_arn')

    print('Deleting DMS assets.')
    dms_client.delete_replication_instance(ReplicationInstanceArn=replication_instance_arn)
    dms_client.delete_endpoint(EndpointArn=source_arn)
    dms_client.delete_endpoint(EndpointArn=target_arn)

    print('Awaiting DMS assets tear-down.')
    dms_client.get_waiter('replication_instance_deleted').wait(
        Filters=[{'Name': 'replication-instance-id', 'Values': [DMS_REPLICATION_INSTANCE_NAME]}]
    )
    dms_client.get_waiter('endpoint_deleted').wait(
        Filters=[
            {
                'Name': 'endpoint-id',
                'Values': [SOURCE_ENDPOINT_IDENTIFIER, TARGET_ENDPOINT_IDENTIFIER],
            }
        ]
    )


with DAG(
    dag_id='example_dms',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    create_db_instance = RdsCreateDbInstanceOperator(
        task_id="create_db_instance",
        db_instance_identifier=RDS_INSTANCE_NAME,
        db_instance_class='db.t3.micro',
        engine=RDS_ENGINE,
        rds_kwargs={
            "DBName": RDS_DB_NAME,
            "AllocatedStorage": 20,
            "MasterUsername": RDS_USERNAME,
            "MasterUserPassword": RDS_PASSWORD,
        },
    )

    # [START howto_operator_dms_create_task]
    create_task = DmsCreateTaskOperator(
        task_id='create_task',
        replication_task_id=DMS_REPLICATION_TASK_ID,
        source_endpoint_arn='{{ ti.xcom_pull(key="source_endpoint_arn") }}',
        target_endpoint_arn='{{ ti.xcom_pull(key="target_endpoint_arn") }}',
        replication_instance_arn='{{ ti.xcom_pull(key="replication_instance_arn") }}',
        table_mappings=TABLE_MAPPINGS,
    )
    # [END howto_operator_dms_create_task]

    # [START howto_operator_dms_start_task]
    start_task = DmsStartTaskOperator(
        task_id='start_task',
        replication_task_arn=create_task.output,
    )
    # [END howto_operator_dms_start_task]

    # [START howto_operator_dms_describe_tasks]
    describe_tasks = DmsDescribeTasksOperator(
        task_id='describe_tasks',
        describe_tasks_kwargs={
            'Filters': [
                {
                    'Name': 'replication-instance-arn',
                    'Values': ['{{ ti.xcom_pull(key="replication_instance_arn") }}'],
                }
            ]
        },
        do_xcom_push=False,
    )
    # [END howto_operator_dms_describe_tasks]

    await_task_start = DmsTaskBaseSensor(
        task_id='await_task_start',
        replication_task_arn=create_task.output,
        target_statuses=['running'],
        termination_statuses=['stopped', 'deleting', 'failed'],
    )

    # [START howto_operator_dms_stop_task]
    stop_task = DmsStopTaskOperator(
        task_id='stop_task',
        replication_task_arn=create_task.output,
    )
    # [END howto_operator_dms_stop_task]

    # TaskCompletedSensor actually waits until task reaches the "Stopped" state, so it will work here.
    # [START howto_sensor_dms_task_completed]
    await_task_stop = DmsTaskCompletedSensor(
        task_id='await_task_stop',
        replication_task_arn=create_task.output,
    )
    # [END howto_sensor_dms_task_completed]

    # [START howto_operator_dms_delete_task]
    delete_task = DmsDeleteTaskOperator(
        task_id='delete_task',
        replication_task_arn=create_task.output,
        trigger_rule='all_done',
    )
    # [END howto_operator_dms_delete_task]

    delete_db_instance = RdsDeleteDbInstanceOperator(
        task_id='delete_db_instance',
        db_instance_identifier=RDS_INSTANCE_NAME,
        rds_kwargs={
            "SkipFinalSnapshot": True,
        },
        trigger_rule='all_done',
    )

    chain(
        create_db_instance,
        create_sample_table(),
        create_dms_assets(),
        create_task,
        start_task,
        describe_tasks,
        await_task_start,
        stop_task,
        await_task_stop,
        delete_task,
        delete_dms_assets(),
        delete_db_instance,
    )
