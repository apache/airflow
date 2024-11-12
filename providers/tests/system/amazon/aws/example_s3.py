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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3DeleteBucketTaggingOperator,
    S3DeleteObjectsOperator,
    S3FileTransformOperator,
    S3GetBucketTaggingOperator,
    S3ListOperator,
    S3ListPrefixesOperator,
    S3PutBucketTaggingOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3KeysUnchangedSensor
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_s3"

sys_test_context_task = SystemTestContextBuilder().build()

DATA = """
    apple,0.5
    milk,2.5
    bread,4.0
"""

# Empty string prefix refers to the bucket root
# See what prefix is here https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
PREFIX = ""
DELIMITER = "/"
TAG_KEY = "test-s3-bucket-tagging-key"
TAG_VALUE = "test-s3-bucket-tagging-value"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    bucket_name = f"{env_id}-s3-bucket"
    bucket_name_2 = f"{env_id}-s3-bucket-2"

    key = f"{env_id}-key"
    key_2 = f"{env_id}-key2"

    key_regex_pattern = ".*-key"

    # [START howto_sensor_s3_key_function_definition]
    def check_fn(files: list, **kwargs) -> bool:
        """
        Example of custom check: check if all files are bigger than ``20 bytes``

        :param files: List of S3 object attributes.
        :return: true if the criteria is met
        """
        return all(f.get("Size", 0) > 20 for f in files)

    # [END howto_sensor_s3_key_function_definition]

    # [START howto_operator_s3_create_bucket]
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )
    # [END howto_operator_s3_create_bucket]

    create_bucket_2 = S3CreateBucketOperator(
        task_id="create_bucket_2",
        bucket_name=bucket_name_2,
    )

    # [START howto_operator_s3_put_bucket_tagging]
    put_tagging = S3PutBucketTaggingOperator(
        task_id="put_tagging",
        bucket_name=bucket_name,
        key=TAG_KEY,
        value=TAG_VALUE,
    )
    # [END howto_operator_s3_put_bucket_tagging]

    # [START howto_operator_s3_get_bucket_tagging]
    get_tagging = S3GetBucketTaggingOperator(
        task_id="get_tagging",
        bucket_name=bucket_name,
    )
    # [END howto_operator_s3_get_bucket_tagging]

    # [START howto_operator_s3_delete_bucket_tagging]
    delete_tagging = S3DeleteBucketTaggingOperator(
        task_id="delete_tagging",
        bucket_name=bucket_name,
    )
    # [END howto_operator_s3_delete_bucket_tagging]

    # [START howto_operator_s3_create_object]
    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=key,
        data=DATA,
        replace=True,
    )
    # [END howto_operator_s3_create_object]

    create_object_2 = S3CreateObjectOperator(
        task_id="create_object_2",
        s3_bucket=bucket_name,
        s3_key=key_2,
        data=DATA,
        replace=True,
    )

    # [START howto_operator_s3_list_prefixes]
    list_prefixes = S3ListPrefixesOperator(
        task_id="list_prefixes",
        bucket=bucket_name,
        prefix=PREFIX,
        delimiter=DELIMITER,
    )
    # [END howto_operator_s3_list_prefixes]

    # [START howto_operator_s3_list]
    list_keys = S3ListOperator(
        task_id="list_keys",
        bucket=bucket_name,
        prefix=PREFIX,
    )
    # [END howto_operator_s3_list]

    # [START howto_sensor_s3_key_single_key]
    # Check if a file exists
    sensor_one_key = S3KeySensor(
        task_id="sensor_one_key",
        bucket_name=bucket_name,
        bucket_key=key,
    )
    # [END howto_sensor_s3_key_single_key]

    # [START howto_sensor_s3_key_multiple_keys]
    # Check if both files exist
    sensor_two_keys = S3KeySensor(
        task_id="sensor_two_keys",
        bucket_name=bucket_name,
        bucket_key=[key, key_2],
    )
    # [END howto_sensor_s3_key_multiple_keys]

    # [START howto_sensor_s3_key_single_key_deferrable]
    # Check if a file exists
    sensor_one_key_deferrable = S3KeySensor(
        task_id="sensor_one_key_deferrable",
        bucket_name=bucket_name,
        bucket_key=key,
        deferrable=True,
    )
    # [END howto_sensor_s3_key_single_key_deferrable]

    # [START howto_sensor_s3_key_multiple_keys_deferrable]
    # Check if both files exist
    sensor_two_keys_deferrable = S3KeySensor(
        task_id="sensor_two_keys_deferrable",
        bucket_name=bucket_name,
        bucket_key=[key, key_2],
        deferrable=True,
    )
    # [END howto_sensor_s3_key_multiple_keys_deferrable]

    # [START howto_sensor_s3_key_function_deferrable]
    # Check if a file exists and match a certain pattern defined in check_fn
    sensor_key_with_function_deferrable = S3KeySensor(
        task_id="sensor_key_with_function_deferrable",
        bucket_name=bucket_name,
        bucket_key=key,
        check_fn=check_fn,
        deferrable=True,
    )
    # [END howto_sensor_s3_key_function_deferrable]

    # [START howto_sensor_s3_key_regex_deferrable]
    # Check if a file exists and match a certain regular expression pattern
    sensor_key_with_regex_deferrable = S3KeySensor(
        task_id="sensor_key_with_regex_deferrable",
        bucket_name=bucket_name,
        bucket_key=key_regex_pattern,
        use_regex=True,
        deferrable=True,
    )
    # [END howto_sensor_s3_key_regex_deferrable]

    # [START howto_sensor_s3_key_function]
    # Check if a file exists and match a certain pattern defined in check_fn
    sensor_key_with_function = S3KeySensor(
        task_id="sensor_key_with_function",
        bucket_name=bucket_name,
        bucket_key=key,
        check_fn=check_fn,
    )
    # [END howto_sensor_s3_key_function]

    # [START howto_sensor_s3_key_regex]
    # Check if a file exists and match a certain regular expression pattern
    sensor_key_with_regex = S3KeySensor(
        task_id="sensor_key_with_regex", bucket_name=bucket_name, bucket_key=key_regex_pattern, use_regex=True
    )
    # [END howto_sensor_s3_key_regex]

    # [START howto_operator_s3_copy_object]
    copy_object = S3CopyObjectOperator(
        task_id="copy_object",
        source_bucket_name=bucket_name,
        dest_bucket_name=bucket_name_2,
        source_bucket_key=key,
        dest_bucket_key=key_2,
    )
    # [END howto_operator_s3_copy_object]

    # [START howto_operator_s3_file_transform]
    file_transform = S3FileTransformOperator(
        task_id="file_transform",
        source_s3_key=f"s3://{bucket_name}/{key}",
        dest_s3_key=f"s3://{bucket_name_2}/{key_2}",
        # Use `cp` command as transform script as an example
        transform_script="cp",
        replace=True,
    )
    # [END howto_operator_s3_file_transform]

    # This task skips the `sensor_keys_unchanged` task because the S3KeysUnchangedSensor
    # runs in poke mode only, which is not supported by the DebugExecutor, causing system tests to fail.
    branching = BranchPythonOperator(
        task_id="branch_to_delete_objects", python_callable=lambda: "delete_objects"
    )

    # [START howto_sensor_s3_keys_unchanged]
    sensor_keys_unchanged = S3KeysUnchangedSensor(
        task_id="sensor_keys_unchanged",
        bucket_name=bucket_name_2,
        prefix=PREFIX,
        inactivity_period=10,  # inactivity_period in seconds
    )
    # [END howto_sensor_s3_keys_unchanged]

    # [START howto_operator_s3_delete_objects]
    delete_objects = S3DeleteObjectsOperator(
        task_id="delete_objects",
        bucket=bucket_name_2,
        keys=key_2,
    )
    # [END howto_operator_s3_delete_objects]
    delete_objects.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_s3_delete_bucket]
    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        force_delete=True,
    )
    # [END howto_operator_s3_delete_bucket]
    delete_bucket.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket_2 = S3DeleteBucketOperator(
        task_id="delete_bucket_2",
        bucket_name=bucket_name_2,
        force_delete=True,
    )
    delete_bucket_2.trigger_rule = TriggerRule.ALL_DONE

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        create_bucket,
        create_bucket_2,
        put_tagging,
        get_tagging,
        delete_tagging,
        create_object,
        create_object_2,
        list_prefixes,
        list_keys,
        [sensor_one_key, sensor_two_keys, sensor_key_with_function, sensor_key_with_regex],
        [
            sensor_one_key_deferrable,
            sensor_two_keys_deferrable,
            sensor_key_with_function_deferrable,
            sensor_key_with_regex_deferrable,
        ],
        copy_object,
        file_transform,
        branching,
        sensor_keys_unchanged,
        # TEST TEARDOWN
        delete_objects,
        delete_bucket,
        delete_bucket_2,
    )
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
