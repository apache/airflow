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
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from botocore.client import BaseClient

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.sensors.glue_catalog_partition import GlueCatalogPartitionSensor
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.common.compat.sdk import DAG, chain, task

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, get_role_name, prune_logs

DAG_ID = "example_glue"

# Externally fetched variables:
# Role needs S3 putobject/getobject access as well as the glue service role,
# see docs here: https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html
ROLE_ARN_KEY = "ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

# Example csv data used as input to the example AWS Glue Job.
EXAMPLE_CSV = """product,value
apple,0.5
milk,2.5
bread,4.0
"""

# Example Spark script to operate on the above sample csv data.
EXAMPLE_SCRIPT = """
from pyspark.context import SparkContext
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())
datasource = glueContext.create_dynamic_frame.from_catalog(
             database='{db_name}', table_name='input')
print('There are %s items in the table' % datasource.count())

datasource.toDF().write.format('csv').mode("append").save('s3://{bucket_name}/output')
"""


@task(trigger_rule=TriggerRule.ALL_DONE)
def glue_cleanup(crawler_name: str, job_name: str, db_name: str) -> None:
    client: BaseClient = boto3.client("glue")

    client.delete_crawler(Name=crawler_name)
    client.delete_job(JobName=job_name)
    client.delete_database(Name=db_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    env_id = test_context[ENV_ID_KEY]
    role_arn = test_context[ROLE_ARN_KEY]
    glue_crawler_name = f"{env_id}_crawler"
    glue_db_name = f"{env_id}_glue_db"
    glue_job_name = f"{env_id}_glue_job"
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
        s3_key="input/category=mixed/input.csv",
        data=EXAMPLE_CSV,
        replace=True,
    )

    upload_script = S3CreateObjectOperator(
        task_id="upload_script",
        s3_bucket=bucket_name,
        s3_key="etl_script.py",
        data=EXAMPLE_SCRIPT.format(db_name=glue_db_name, bucket_name=bucket_name),
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

    # [START howto_sensor_glue_catalog_partition]
    wait_for_catalog_partition = GlueCatalogPartitionSensor(
        task_id="wait_for_catalog_partition",
        table_name="input",
        database_name=glue_db_name,
        expression="category='mixed'",
    )
    # [END howto_sensor_glue_catalog_partition]

    # [START howto_operator_glue]
    submit_glue_job = GlueJobOperator(
        task_id="submit_glue_job",
        job_name=glue_job_name,
        script_location=f"s3://{bucket_name}/etl_script.py",
        s3_bucket=bucket_name,
        iam_role_name=role_name,
        create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
    )
    # [END howto_operator_glue]

    # GlueJobOperator waits by default, setting as False to test the Sensor below.
    submit_glue_job.wait_for_completion = False

    # [START howto_sensor_glue]
    wait_for_job = GlueJobSensor(
        task_id="wait_for_job",
        job_name=glue_job_name,
        # Job ID extracted from previous Glue Job Operator task
        run_id=submit_glue_job.output,
        verbose=True,  # prints glue job logs in airflow logs
    )
    # [END howto_sensor_glue]
    wait_for_job.poke_interval = 5

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
            ("/aws-glue/jobs/logs-v2", submit_glue_job.output),
            ("/aws-glue/jobs/error", submit_glue_job.output),
            ("/aws-glue/jobs/output", submit_glue_job.output),
        ],
        delete_log_groups=False,
    )

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        upload_csv,
        upload_script,
        # TEST BODY
        crawl_s3,
        wait_for_crawl,
        wait_for_catalog_partition,
        submit_glue_job,
        wait_for_job,
        # TEST TEARDOWN
        glue_cleanup(glue_crawler_name, glue_job_name, glue_db_name),
        delete_bucket,
        log_cleanup,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
