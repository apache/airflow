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
    OSSDeleteBatchObjectOperator,
    OSSDeleteBucketOperator,
    OSSDeleteObjectOperator,
    OSSDownloadObjectOperator,
    OSSUploadObjectOperator,
)
from tests.providers.alibaba.cloud.utils.test_utils import skip_test_if_no_valid_conn_id

TEST_CONN_ID = os.environ.get('TEST_OSS_CONN_ID', 'oss_default')
TEST_REGION = os.environ.get('TEST_OSS_REGION', 'cn-hangzhou')
TEST_BUCKET = os.environ.get('TEST_OSS_BUCKET', 'test-bucket')
TEST_FILE_PATH = '/tmp/airflow-test'


class TestOSSOperator(unittest.TestCase):
    def setUp(self):
        self.create_bucket_operator = OSSCreateBucketOperator(
            oss_conn_id=TEST_CONN_ID, region=TEST_REGION, bucket_name=TEST_BUCKET, task_id='task-1'
        )
        self.delete_bucket_operator = OSSDeleteBucketOperator(
            oss_conn_id=TEST_CONN_ID, region=TEST_REGION, bucket_name=TEST_BUCKET, task_id='task-2'
        )
        try:
            self.hook = OSSHook(region=TEST_REGION)
            self.hook.object_exists(key='test-obj', bucket_name=TEST_BUCKET)
        except AirflowException:
            self.hook = None
        except oss2.exceptions.ServerError as e:
            if e.status == 403:
                self.hook = None

    @skip_test_if_no_valid_conn_id
    def test_init(self):
        assert self.create_bucket_operator.oss_conn_id == TEST_CONN_ID

    @skip_test_if_no_valid_conn_id
    def test_create_delete_bucket(self):
        self.create_bucket_operator.execute({})
        self.delete_bucket_operator.execute({})

    @skip_test_if_no_valid_conn_id
    def test_object(self):
        self.create_bucket_operator.execute({})

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
            task_id='task-1',
        )
        upload_object_operator.execute({})
        assert self.hook.object_exists(key='obj', bucket_name=TEST_BUCKET)

        download_file = f'{TEST_FILE_PATH}_download_1'
        download_object_operator = OSSDownloadObjectOperator(
            key='obj',
            file=download_file,
            oss_conn_id=TEST_CONN_ID,
            region=TEST_REGION,
            bucket_name=TEST_BUCKET,
            task_id='task-2',
        )
        download_object_operator.execute({})
        assert os.path.exists(download_file)

        delete_object_operator = OSSDeleteObjectOperator(
            key='obj', oss_conn_id=TEST_CONN_ID, region=TEST_REGION, bucket_name=TEST_BUCKET, task_id='task-3'
        )
        delete_object_operator.execute({})
        assert self.hook.object_exists(key='obj', bucket_name=TEST_BUCKET) is False

        upload_object_operator = OSSUploadObjectOperator(
            key='obj',
            file=upload_file,
            oss_conn_id=TEST_CONN_ID,
            region=TEST_REGION,
            bucket_name=TEST_BUCKET,
            task_id='task-4',
        )
        upload_object_operator.execute({})
        assert self.hook.object_exists(key='obj', bucket_name=TEST_BUCKET)

        delete_objects_operator = OSSDeleteBatchObjectOperator(
            keys=['obj'],
            oss_conn_id=TEST_CONN_ID,
            region=TEST_REGION,
            bucket_name=TEST_BUCKET,
            task_id='task-5',
        )
        delete_objects_operator.execute({})
        assert self.hook.object_exists(key='obj', bucket_name=TEST_BUCKET) is False

        self.delete_bucket_operator.execute({})
