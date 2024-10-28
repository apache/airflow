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

import pytest

from airflow.jobs.triggerer_job_runner import TriggerRunner
from airflow.models import Trigger
from airflow.providers.amazon.aws.triggers.athena import AthenaTrigger
from airflow.providers.amazon.aws.triggers.batch import BatchCreateComputeEnvironmentTrigger, BatchJobTrigger
from airflow.providers.amazon.aws.triggers.ecs import ClusterActiveTrigger, ClusterInactiveTrigger
from airflow.providers.amazon.aws.triggers.eks import (
    EksCreateClusterTrigger,
    EksCreateFargateProfileTrigger,
    EksCreateNodegroupTrigger,
    EksDeleteClusterTrigger,
    EksDeleteFargateProfileTrigger,
    EksDeleteNodegroupTrigger,
)
from airflow.providers.amazon.aws.triggers.emr import (
    EmrAddStepsTrigger,
    EmrContainerTrigger,
    EmrCreateJobFlowTrigger,
    EmrServerlessCancelJobsTrigger,
    EmrServerlessCreateApplicationTrigger,
    EmrServerlessDeleteApplicationTrigger,
    EmrServerlessStartApplicationTrigger,
    EmrServerlessStartJobTrigger,
    EmrServerlessStopApplicationTrigger,
    EmrStepSensorTrigger,
    EmrTerminateJobFlowTrigger,
)
from airflow.providers.amazon.aws.triggers.glue import GlueCatalogPartitionTrigger
from airflow.providers.amazon.aws.triggers.glue_crawler import GlueCrawlerCompleteTrigger
from airflow.providers.amazon.aws.triggers.lambda_function import LambdaCreateFunctionCompleteTrigger
from airflow.providers.amazon.aws.triggers.rds import (
    RdsDbAvailableTrigger,
    RdsDbDeletedTrigger,
    RdsDbStoppedTrigger,
)
from airflow.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftCreateClusterSnapshotTrigger,
    RedshiftCreateClusterTrigger,
    RedshiftDeleteClusterTrigger,
    RedshiftPauseClusterTrigger,
    RedshiftResumeClusterTrigger,
)
from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger
from airflow.providers.amazon.aws.triggers.step_function import StepFunctionsExecutionCompleteTrigger
from airflow.providers.amazon.aws.utils.rds import RdsDbType
from airflow.serialization.serialized_objects import BaseSerialization

pytestmark = pytest.mark.db_test

BATCH_JOB_ID = "job_id"

TEST_CLUSTER_IDENTIFIER = "test-cluster"
TEST_FARGATE_PROFILE_NAME = "test-fargate-profile"
TEST_NODEGROUP_NAME = "test-nodegroup"

TEST_JOB_FLOW_ID = "test-job-flow-id"
VIRTUAL_CLUSTER_ID = "vzwemreks"
JOB_ID = "job-1234"
TARGET_STATE = ["TERMINATED"]
STEP_ID = "s-1234"

TEST_APPLICATION_ID = "test-application-id"
TEST_JOB_ID = "test-job-id"

TEST_FUNCTION_NAME = "test-function-name"

TEST_DB_INSTANCE_IDENTIFIER = "test-db-instance-identifier"
TEST_RESPONSE = {
    "DBInstance": {
        "DBInstanceIdentifier": "test-db-instance-identifier",
        "DBInstanceStatus": "test-db-instance-status",
    }
}

TEST_SQS_QUEUE = "test-sqs-queue"
TEST_MAX_MESSAGES = 1
TEST_NUM_BATCHES = 1
TEST_WAIT_TIME_SECONDS = 1
TEST_VISIBILITY_TIMEOUT = 1
TEST_MESSAGE_FILTERING_MATCH_VALUES = "test"
TEST_MESSAGE_FILTERING_CONFIG = "test-message-filtering-config"
TEST_DELETE_MESSAGE_ON_RECEPTION = False

TEST_ARN = "test-aws-arn"

WAITER_DELAY = 5
MAX_ATTEMPTS = 5
AWS_CONN_ID = "aws_batch_job_conn"
AWS_REGION = "us-east-2"


pytest.importorskip("aiobotocore")


def gen_test_name(trigger):
    """Gives to tests the name of the class being tested."""
    return trigger.__class__.__name__


class TestTriggersSerialization:
    @pytest.mark.parametrize(
        "trigger",
        [
            AthenaTrigger("query_id", 1, 5, "aws connection"),
            BatchJobTrigger(
                job_id=BATCH_JOB_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
            ),
            BatchCreateComputeEnvironmentTrigger(
                compute_env_arn="my_arn",
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
            ),
            ClusterActiveTrigger(
                cluster_arn="my_arn",
                aws_conn_id="my_conn",
                waiter_delay=1,
                waiter_max_attempts=2,
                region_name="my_region",
            ),
            ClusterInactiveTrigger(
                cluster_arn="my_arn",
                aws_conn_id="my_conn",
                waiter_delay=1,
                waiter_max_attempts=2,
                region_name="my_region",
            ),
            EksCreateFargateProfileTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EksDeleteFargateProfileTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EksCreateNodegroupTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                nodegroup_name=TEST_NODEGROUP_NAME,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                region_name=AWS_REGION,
            ),
            EksDeleteNodegroupTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                nodegroup_name=TEST_NODEGROUP_NAME,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                region_name=AWS_REGION,
            ),
            EksCreateClusterTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=WAITER_DELAY,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
            ),
            EksDeleteClusterTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=WAITER_DELAY,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
                force_delete_compute=True,
            ),
            EmrAddStepsTrigger(
                job_flow_id=TEST_JOB_FLOW_ID,
                step_ids=["my_step1", "my_step2"],
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EmrCreateJobFlowTrigger(
                job_flow_id=TEST_JOB_FLOW_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EmrTerminateJobFlowTrigger(
                job_flow_id=TEST_JOB_FLOW_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EmrContainerTrigger(
                virtual_cluster_id=VIRTUAL_CLUSTER_ID,
                job_id=JOB_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
            ),
            EmrStepSensorTrigger(
                job_flow_id=TEST_JOB_FLOW_ID,
                step_id=STEP_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
            ),
            EmrServerlessCreateApplicationTrigger(
                application_id=TEST_APPLICATION_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EmrServerlessStartApplicationTrigger(
                application_id=TEST_APPLICATION_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EmrServerlessStopApplicationTrigger(
                application_id=TEST_APPLICATION_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EmrServerlessDeleteApplicationTrigger(
                application_id=TEST_APPLICATION_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EmrServerlessCancelJobsTrigger(
                application_id=TEST_APPLICATION_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            EmrServerlessStartJobTrigger(
                application_id=TEST_APPLICATION_ID,
                job_id=TEST_JOB_ID,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            GlueCrawlerCompleteTrigger(crawler_name="my_crawler", waiter_delay=2, aws_conn_id="my_conn_id"),
            GlueCatalogPartitionTrigger(
                database_name="my_database",
                table_name="my_table",
                expression="my_expression",
                aws_conn_id="my_conn_id",
            ),
            LambdaCreateFunctionCompleteTrigger(
                function_name=TEST_FUNCTION_NAME,
                function_arn=TEST_ARN,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
            ),
            RedshiftCreateClusterTrigger(
                cluster_identifier=TEST_CLUSTER_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
            ),
            RedshiftPauseClusterTrigger(
                cluster_identifier=TEST_CLUSTER_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
            ),
            RedshiftCreateClusterSnapshotTrigger(
                cluster_identifier=TEST_CLUSTER_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
            ),
            RedshiftResumeClusterTrigger(
                cluster_identifier=TEST_CLUSTER_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
            ),
            RedshiftDeleteClusterTrigger(
                cluster_identifier=TEST_CLUSTER_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
            ),
            RdsDbAvailableTrigger(
                db_identifier=TEST_DB_INSTANCE_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
                response=TEST_RESPONSE,
                db_type=RdsDbType.INSTANCE,
            ),
            RdsDbDeletedTrigger(
                db_identifier=TEST_DB_INSTANCE_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
                response=TEST_RESPONSE,
                db_type=RdsDbType.INSTANCE,
            ),
            RdsDbStoppedTrigger(
                db_identifier=TEST_DB_INSTANCE_IDENTIFIER,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
                response=TEST_RESPONSE,
                db_type=RdsDbType.INSTANCE,
            ),
            SqsSensorTrigger(
                sqs_queue=TEST_SQS_QUEUE,
                aws_conn_id=AWS_CONN_ID,
                max_messages=TEST_MAX_MESSAGES,
                num_batches=TEST_NUM_BATCHES,
                wait_time_seconds=TEST_WAIT_TIME_SECONDS,
                visibility_timeout=TEST_VISIBILITY_TIMEOUT,
                message_filtering="literal",
                message_filtering_match_values=TEST_MESSAGE_FILTERING_MATCH_VALUES,
                message_filtering_config=TEST_MESSAGE_FILTERING_CONFIG,
                delete_message_on_reception=TEST_DELETE_MESSAGE_ON_RECEPTION,
                waiter_delay=WAITER_DELAY,
            ),
            StepFunctionsExecutionCompleteTrigger(
                execution_arn=TEST_ARN,
                aws_conn_id=AWS_CONN_ID,
                waiter_delay=WAITER_DELAY,
                waiter_max_attempts=MAX_ATTEMPTS,
                region_name=AWS_REGION,
            ),
        ],
        ids=gen_test_name,
    )
    def test_serialize_recreate(self, trigger):
        # generate the DB object from the trigger
        trigger_db: Trigger = Trigger.from_object(trigger)

        # serialize/deserialize using the same method that is used when inserting in DB
        json_params = BaseSerialization.serialize(trigger_db.kwargs)
        retrieved_params = BaseSerialization.deserialize(json_params)

        # recreate a new trigger object from the data we would have in DB
        clazz = TriggerRunner().get_trigger_by_classpath(trigger_db.classpath)
        # noinspection PyArgumentList
        instance = clazz(**retrieved_params)

        # recreate a DB column object from the new trigger so that we can easily compare attributes
        trigger_db_2: Trigger = Trigger.from_object(instance)

        assert trigger_db.classpath == trigger_db_2.classpath
        assert trigger_db.kwargs == trigger_db_2.kwargs
