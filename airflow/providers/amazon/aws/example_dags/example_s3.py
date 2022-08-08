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

import os
from datetime import datetime
from typing import List

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

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'test-airflow-12345')
BUCKET_NAME_2 = os.environ.get('BUCKET_NAME_2', 'test-airflow-123456')
KEY = os.environ.get('KEY', 'key')
KEY_2 = os.environ.get('KEY_2', 'key2')
# Empty string prefix refers to the bucket root
# See what prefix is here https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
PREFIX = os.environ.get('PREFIX', '')
DELIMITER = os.environ.get('DELIMITER', '/')
TAG_KEY = os.environ.get('TAG_KEY', 'test-s3-bucket-tagging-key')
TAG_VALUE = os.environ.get('TAG_VALUE', 'test-s3-bucket-tagging-value')
DATA = os.environ.get(
    'DATA',
    '''
apple,0.5
milk,2.5
bread,4.0
''',
)

with DAG(
    dag_id='example_s3',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_sensor_s3_key_function_definition]
    def check_fn(files: List) -> bool:
        """
        Example of custom check: check if all files are bigger than ``1kB``

        :param files: List of S3 object attributes.
        :return: true if the criteria is met
        :rtype: bool
        """
        return all(f.get('Size', 0) > 1024 for f in files)

    # [END howto_sensor_s3_key_function_definition]

    # [START howto_operator_s3_create_bucket]
    create_bucket = S3CreateBucketOperator(
        task_id='s3_create_bucket',
        bucket_name=BUCKET_NAME,
    )
    # [END howto_operator_s3_create_bucket]

    # [START howto_operator_s3_put_bucket_tagging]
    put_tagging = S3PutBucketTaggingOperator(
        task_id='s3_put_bucket_tagging',
        bucket_name=BUCKET_NAME,
        key=TAG_KEY,
        value=TAG_VALUE,
    )
    # [END howto_operator_s3_put_bucket_tagging]

    # [START howto_operator_s3_get_bucket_tagging]
    get_tagging = S3GetBucketTaggingOperator(
        task_id='s3_get_bucket_tagging',
        bucket_name=BUCKET_NAME,
    )
    # [END howto_operator_s3_get_bucket_tagging]

    # [START howto_operator_s3_delete_bucket_tagging]
    delete_tagging = S3DeleteBucketTaggingOperator(
        task_id='s3_delete_bucket_tagging',
        bucket_name=BUCKET_NAME,
    )
    # [END howto_operator_s3_delete_bucket_tagging]

    # [START howto_operator_s3_create_object]
    create_object = S3CreateObjectOperator(
        task_id="s3_create_object",
        s3_bucket=BUCKET_NAME,
        s3_key=KEY,
        data=DATA,
        replace=True,
    )
    # [END howto_operator_s3_create_object]

    # [START howto_operator_s3_list_prefixes]
    list_prefixes = S3ListPrefixesOperator(
        task_id="s3_list_prefix_operator",
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        delimiter=DELIMITER,
    )
    # [END howto_operator_s3_list_prefixes]

    # [START howto_operator_s3_list]
    list_keys = S3ListOperator(
        task_id="s3_list_operator",
        bucket=BUCKET_NAME,
        prefix=PREFIX,
    )
    # [END howto_operator_s3_list]

    # [START howto_sensor_s3_key_single_key]
    # Check if a file exists
    sensor_one_key = S3KeySensor(
        task_id="s3_sensor_one_key",
        bucket_name=BUCKET_NAME,
        bucket_key=KEY,
    )
    # [END howto_sensor_s3_key_single_key]

    # [START howto_sensor_s3_key_multiple_keys]
    # Check if both files exist
    sensor_two_keys = S3KeySensor(
        task_id="s3_sensor_two_keys",
        bucket_name=BUCKET_NAME,
        bucket_key=[KEY, KEY_2],
    )
    # [END howto_sensor_s3_key_multiple_keys]

    # [START howto_sensor_s3_key_function]
    # Check if a file exists and match a certain pattern defined in check_fn
    sensor_key_with_function = S3KeySensor(
        task_id="s3_sensor_key_function",
        bucket_name=BUCKET_NAME,
        bucket_key=KEY,
        check_fn=check_fn,
    )
    # [END howto_sensor_s3_key_function]

    # [START howto_sensor_s3_keys_unchanged]
    sensor_keys_unchanged = S3KeysUnchangedSensor(
        task_id="s3_sensor_one_key_size",
        bucket_name=BUCKET_NAME_2,
        prefix=PREFIX,
        inactivity_period=10,
    )
    # [END howto_sensor_s3_keys_unchanged]

    # [START howto_operator_s3_copy_object]
    copy_object = S3CopyObjectOperator(
        task_id="s3_copy_object",
        source_bucket_name=BUCKET_NAME,
        dest_bucket_name=BUCKET_NAME_2,
        source_bucket_key=KEY,
        dest_bucket_key=KEY_2,
    )
    # [END howto_operator_s3_copy_object]

    # [START howto_operator_s3_file_transform]
    transforms_file = S3FileTransformOperator(
        task_id="s3_file_transform",
        source_s3_key=f's3://{BUCKET_NAME}/{KEY}',
        dest_s3_key=f's3://{BUCKET_NAME_2}/{KEY_2}',
        # Use `cp` command as transform script as an example
        transform_script='cp',
        replace=True,
    )
    # [END howto_operator_s3_file_transform]

    # [START howto_operator_s3_delete_objects]
    delete_objects = S3DeleteObjectsOperator(
        task_id="s3_delete_objects",
        bucket=BUCKET_NAME_2,
        keys=KEY_2,
    )
    # [END howto_operator_s3_delete_objects]

    # [START howto_operator_s3_delete_bucket]
    delete_bucket = S3DeleteBucketOperator(
        task_id='s3_delete_bucket', bucket_name=BUCKET_NAME, force_delete=True
    )
    # [END howto_operator_s3_delete_bucket]

    chain(
        create_bucket,
        put_tagging,
        get_tagging,
        delete_tagging,
        create_object,
        list_prefixes,
        list_keys,
        [sensor_one_key, sensor_two_keys, sensor_key_with_function],
        copy_object,
        transforms_file,
        sensor_keys_unchanged,
        delete_objects,
        delete_bucket,
    )
