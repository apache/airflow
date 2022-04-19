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
    S3GetBucketTaggingOperator,
    S3PutBucketTaggingOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'test-airflow-12345')
BUCKET_NAME_2 = os.environ.get('BUCKET_NAME_2', 'test-airflow-123456')
KEY = os.environ.get('KEY', 'key')
KEY_2 = os.environ.get('KEY_2', 'key2')
TAG_KEY = os.environ.get('TAG_KEY', 'test-s3-bucket-tagging-key')
TAG_VALUE = os.environ.get('TAG_VALUE', 'test-s3-bucket-tagging-value')
KEY = os.environ.get('KEY', 'key')
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
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_sensor_s3_key_function_definition]
    def check_fn(files: List) -> bool:
        """
        Example of custom check: check if all files are bigger than 1kB

        :param files: List of S3 object attributes.
        Format: [{
            'Size': int
        }]
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
    s3_create_object = S3CreateObjectOperator(
        task_id="s3_create_object",
        s3_bucket=BUCKET_NAME,
        s3_key=KEY,
        data=DATA,
        replace=True,
    )
    # [END howto_operator_s3_create_object]

    # [START howto_sensor_s3_key_single_key]
    # Check if a file exists
    s3_sensor_one_key = S3KeySensor(
        task_id="s3_sensor_one_key",
        bucket_name=BUCKET_NAME,
        bucket_key=KEY,
    )
    # [END howto_sensor_s3_key_single_key]

    # [START howto_sensor_s3_key_multiple_keys]
    # Check if both files exist
    s3_sensor_two_keys = S3KeySensor(
        task_id="s3_sensor_two_keys",
        bucket_name=BUCKET_NAME,
        bucket_key=[KEY, KEY_2],
    )
    # [END howto_sensor_s3_key_multiple_keys]

    # [START howto_sensor_s3_key_function]
    # Check if a file exists and match a certain pattern defined in check_fn
    s3_sensor_key_function = S3KeySensor(
        task_id="s3_sensor_key_function",
        bucket_name=BUCKET_NAME,
        bucket_key=KEY,
        check_fn=check_fn,
    )
    # [END howto_sensor_s3_key_function]

    # [START howto_operator_s3_copy_object]
    s3_copy_object = S3CopyObjectOperator(
        task_id="s3_copy_object",
        source_bucket_name=BUCKET_NAME,
        dest_bucket_name=BUCKET_NAME_2,
        source_bucket_key=KEY,
        dest_bucket_key=KEY_2,
    )
    # [END howto_operator_s3_copy_object]

    # [START howto_operator_s3_delete_objects]
    s3_delete_objects = S3DeleteObjectsOperator(
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
        s3_create_object,
        [s3_sensor_one_key, s3_sensor_two_keys, s3_sensor_key_function],
        s3_copy_object,
        s3_delete_objects,
        delete_bucket,
    )
