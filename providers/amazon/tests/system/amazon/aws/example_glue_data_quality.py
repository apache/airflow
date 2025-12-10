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

from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue import (
    GlueDataQualityOperator,
    GlueDataQualityRuleSetEvaluationRunOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.glue import GlueDataQualityRuleSetEvaluationRunSensor

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task, task_group
else:
    # Airflow 2 path
    from airflow.decorators import task, task_group  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

DAG_ID = "example_glue_data_quality"
SAMPLE_DATA = """"Alice",20
    "Bob",25
    "Charlie",30"""
SAMPLE_FILENAME = "airflow_sample.csv"

RULE_SET = """
Rules = [
    RowCount between 2 and 8,
    IsComplete "name",
    Uniqueness "name" > 0.95,
    ColumnLength "name" between 3 and 14,
    ColumnValues "age" between 19 and 31
]
"""


@task_group
def glue_data_quality_workflow():
    # [START howto_operator_glue_data_quality_operator]
    create_rule_set = GlueDataQualityOperator(
        task_id="create_rule_set",
        name=rule_set_name,
        ruleset=RULE_SET,
        data_quality_ruleset_kwargs={
            "TargetTable": {
                "TableName": athena_table,
                "DatabaseName": athena_database,
            }
        },
    )
    # [END howto_operator_glue_data_quality_operator]

    # [START howto_operator_glue_data_quality_ruleset_evaluation_run_operator]
    start_evaluation_run = GlueDataQualityRuleSetEvaluationRunOperator(
        task_id="start_evaluation_run",
        datasource={
            "GlueTable": {
                "TableName": athena_table,
                "DatabaseName": athena_database,
            }
        },
        role=test_context[ROLE_ARN_KEY],
        rule_set_names=[rule_set_name],
    )
    # [END howto_operator_glue_data_quality_ruleset_evaluation_run_operator]
    start_evaluation_run.wait_for_completion = False

    # [START howto_sensor_glue_data_quality_ruleset_evaluation_run]
    await_evaluation_run_sensor = GlueDataQualityRuleSetEvaluationRunSensor(
        task_id="await_evaluation_run_sensor",
        evaluation_run_id=start_evaluation_run.output,
    )
    # [END howto_sensor_glue_data_quality_ruleset_evaluation_run]

    chain(create_rule_set, start_evaluation_run, await_evaluation_run_sensor)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_ruleset(ruleset_name):
    hook = GlueDataQualityHook()
    hook.conn.delete_data_quality_ruleset(Name=ruleset_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    rule_set_name = f"{env_id}-system-test-ruleset"
    s3_bucket = f"{env_id}-glue-dq-athena-bucket"
    athena_table = f"{env_id}_test_glue_dq_table"
    athena_database = f"{env_id}_glue_dq_default"

    query_create_database = f"CREATE DATABASE IF NOT EXISTS {athena_database}"
    query_create_table = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {athena_database}.{athena_table}
            ( `name` string, `age` int )
            ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            WITH SERDEPROPERTIES ( "serialization.format" = ",", "field.delim" = "," )
            LOCATION "s3://{s3_bucket}//{athena_table}"
            TBLPROPERTIES ("has_encrypted_data"="false")
            """
    query_read_table = f"SELECT * from {athena_database}.{athena_table}"
    query_drop_table = f"DROP TABLE IF EXISTS {athena_database}.{athena_table}"
    query_drop_database = f"DROP DATABASE IF EXISTS {athena_database}"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    upload_sample_data = S3CreateObjectOperator(
        task_id="upload_sample_data",
        s3_bucket=s3_bucket,
        s3_key=f"{athena_table}/{SAMPLE_FILENAME}",
        data=SAMPLE_DATA,
        replace=True,
    )

    create_database = AthenaOperator(
        task_id="create_database",
        query=query_create_database,
        database=athena_database,
        output_location=f"s3://{s3_bucket}/",
        sleep_time=1,
    )

    create_table = AthenaOperator(
        task_id="create_table",
        query=query_create_table,
        database=athena_database,
        output_location=f"s3://{s3_bucket}/",
        sleep_time=1,
    )

    drop_table = AthenaOperator(
        task_id="drop_table",
        query=query_drop_table,
        database=athena_database,
        output_location=f"s3://{s3_bucket}/",
        trigger_rule=TriggerRule.ALL_DONE,
        sleep_time=1,
    )

    drop_database = AthenaOperator(
        task_id="drop_database",
        query=query_drop_database,
        database=athena_database,
        output_location=f"s3://{s3_bucket}/",
        trigger_rule=TriggerRule.ALL_DONE,
        sleep_time=1,
    )

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket,
        upload_sample_data,
        create_database,
        create_table,
        # TEST BODY
        glue_data_quality_workflow(),
        # TEST TEARDOWN
        delete_ruleset(rule_set_name),
        drop_table,
        drop_database,
        delete_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    #     This test needs watcher in order to properly mark success/failure
    #     when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
