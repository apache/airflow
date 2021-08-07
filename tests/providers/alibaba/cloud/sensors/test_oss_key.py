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
#
import os
import unittest

import oss2

from airflow.exceptions import AirflowException
from airflow.providers.alibaba.cloud.hooks.oss import OSSHook
from airflow.providers.alibaba.cloud.operators.oss import (
    OSSCreateBucketOperator,
    OSSDeleteBucketOperator,
    OSSDeleteObjectOperator,
    OSSUploadObjectOperator,
)
from airflow.providers.alibaba.cloud.sensors.oss_key import OSSKeySensor
from tests.providers.alibaba.cloud.utils.test_utils import skip_test_if_no_valid_conn_id

TEST_CONN_ID = os.environ.get('TEST_OSS_CONN_ID', 'oss_default')
TEST_REGION = os.environ.get('TEST_OSS_REGION', 'cn-hangzhou')
TEST_BUCKET = os.environ.get('TEST_OSS_BUCKET', 'test-bucket')
TEST_FILE_PATH = '/tmp/airflow-test'


class TestOSSSensor(unittest.TestCase):
    def setUp(self):
        self.sensor = OSSKeySensor(
            bucket_key='obj',
            oss_conn_id=TEST_CONN_ID,
            region=TEST_REGION,
            bucket_name=TEST_BUCKET,
            task_id='task-1',
        )
        try:
            self.hook = OSSHook(region=TEST_REGION, oss_conn_id=TEST_CONN_ID)
            self.hook.object_exists(key='test-obj', bucket_name=TEST_BUCKET)
        except AirflowException:
            self.hook = None
        except oss2.exceptions.ServerError as e:
            if e.status == 403:
                self.hook = None

    @skip_test_if_no_valid_conn_id
    def test_init(self):
        assert self.sensor.oss_conn_id == TEST_CONN_ID

    @skip_test_if_no_valid_conn_id
    def test_poke(self):
        create_bucket_operator = OSSCreateBucketOperator(
            oss_conn_id=TEST_CONN_ID, region=TEST_REGION, bucket_name=TEST_BUCKET, task_id='task-2'
        )
        create_bucket_operator.execute({})

        upload_file = f'{TEST_FILE_PATH}_upload_1'
        if not os.path.exists(upload_file):
            with open(upload_file, 'w') as f:
                f.write('test')
        upload_object_operator = OSSUploadObjectOperator(
            key='obj',
            file=upload_file,
            oss_conn_id=TEST_CONN_ID,
            region=TEST_REGION,
            bucket_name=TEST_BUCKET,
            task_id='task-3',
        )
        upload_object_operator.execute({})
        assert self.sensor.poke({})

        delete_object_operator = OSSDeleteObjectOperator(
            key='obj', oss_conn_id=TEST_CONN_ID, region=TEST_REGION, bucket_name=TEST_BUCKET, task_id='task-4'
        )
        delete_object_operator.execute({})

        delete_bucket_operator = OSSDeleteBucketOperator(
            oss_conn_id=TEST_CONN_ID, region=TEST_REGION, bucket_name=TEST_BUCKET, task_id='task-5'
        )
        delete_bucket_operator.execute({})
