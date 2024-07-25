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

import logging
from datetime import datetime
from typing import TYPE_CHECKING

import boto3

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue_session import (
    GlueCreateSessionOperator,
    GlueDeleteSessionOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, prune_logs

if TYPE_CHECKING:
    from botocore.client import BaseClient

logger = logging.getLogger(__name__)

DAG_ID = "example_glue_session"

# Externally fetched variables:
# Role needs S3 putobject/getobject access as well as the glue service role,
# see docs here: https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html
ROLE_ARN_KEY = "ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

# Example csv data used as input to the example AWS Glue Job.
EXAMPLE_CSV = """
apple,0.5
milk,2.5
bread,4.0
"""

SQL_STATEMENT = "SELECT * FROM `{db_name}`.`input`;"


@task
def get_role_name(arn: str) -> str:
    return arn.split("/")[-1]


@task
def run_statement(session_id: str, db_name: str):
    code = SQL_STATEMENT.format(db_name=db_name)
    client: BaseClient = boto3.client("glue")

    statement = client.run_statement(SessionId=session_id, Code=code)
    result = client.get_statement(SessionId=session_id, Id=statement["Id"])
    logger.info("Output data: %s", result["Output"]["Data"]["TextPlain"])


@task(trigger_rule=TriggerRule.ALL_DONE)
def glue_cleanup(crawler_name: str, db_name: str) -> None:
    client: BaseClient = boto3.client("glue")

    client.delete_crawler(Name=crawler_name)
    client.delete_database(Name=db_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    env_id = test_context[ENV_ID_KEY]
    role_arn = test_context[ROLE_ARN_KEY]
    glue_crawler_name = f"{env_id}_crawler"
    glue_db_name = f"{env_id}_glue_db"
    glue_session_id = f"{env_id}_glue_session"
    bucket_name = f"{env_id}-bucket"
    role_name = get_role_name(role_arn)

    glue_crawler_config = {
        "Name": glue_crawler_name,
        "Role": role_arn,
        "DatabaseName": glue_db_name,
        "Targets": {"S3Targets": [{"Path": f"{bucket_name}/input"}]},
    }

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    upload_csv = S3CreateObjectOperator(
        task_id="upload_csv",
        s3_bucket=bucket_name,
        s3_key="input/input.csv",
        data=EXAMPLE_CSV,
        replace=True,
    )

    # [START howto_operator_glue_crawler]
    crawl_s3 = GlueCrawlerOperator(
        task_id="crawl_s3",
        config=glue_crawler_config,
    )
    # [END howto_operator_glue_crawler]

    # GlueCrawlerOperator waits by default, setting as False to test the Sensor below.
    crawl_s3.wait_for_completion = False

    # [START howto_sensor_glue_crawler]
    wait_for_crawl = GlueCrawlerSensor(
        task_id="wait_for_crawl",
        crawler_name=glue_crawler_name,
    )
    # [END howto_sensor_glue_crawler]
    wait_for_crawl.timeout = 500

    # [START howto_operator_glue_session]
    create_glue_session = GlueCreateSessionOperator(
        task_id="create_glue_session",
        session_id=glue_session_id,
        iam_role_name=role_name,
        create_session_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
    )
    # [END howto_operator_glue_session]

    # [START howto_operator_glue_session]
    delete_glue_session = GlueDeleteSessionOperator(
        task_id="delete_glue_session",
        session_id=glue_session_id,
    )
    # [END howto_operator_glue_session]

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )

    log_cleanup = prune_logs(
        [
            # Format: ('log group name', 'log stream prefix')
            ("/aws-glue/crawlers", glue_crawler_name),
        ]
    )

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        upload_csv,
        # TEST BODY
        crawl_s3,
        wait_for_crawl,
        create_glue_session,
        run_statement(glue_session_id, glue_db_name),
        delete_glue_session,
        # TEST TEARDOWN
        glue_cleanup(glue_crawler_name, glue_db_name),
        delete_bucket,
        log_cleanup,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
