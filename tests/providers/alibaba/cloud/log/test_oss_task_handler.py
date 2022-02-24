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
from airflow.providers.alibaba.cloud.log.oss_task_handler import OSSTaskHandler
from tests.providers.alibaba.cloud.utils.test_utils import skip_test_if_no_valid_conn_id
from tests.test_utils.config import conf_vars

TEST_CONN_ID = os.environ.get('TEST_OSS_CONN_ID', 'oss_default')
TEST_REGION = os.environ.get('TEST_OSS_REGION', 'us-east-1')
TEST_BUCKET = os.environ.get('TEST_OSS_BUCKET', 'test-bucket')


class TestOSSTaskHandler(unittest.TestCase):
    @conf_vars({('logging', 'remote_log_conn_id'): 'oss_default'})
    def setUp(self):
        self.remote_log_base = f'oss://{TEST_BUCKET}/airflow/logs'
        self.remote_log_location = f'oss://{TEST_BUCKET}/airflow/logs/1.log'
        self.local_log_location = 'local/airflow/logs/1.log'
        self.filename_template = '{try_number}.log'
        self.remote_log_key = 'airflow/logs/1.log'
        try:
            self.hook = OSSHook(region=TEST_REGION, oss_conn_id=TEST_CONN_ID)
            self.hook.object_exists(key='test-obj', bucket_name=TEST_BUCKET)
            self.oss_task_handler = OSSTaskHandler(
                self.local_log_location, self.remote_log_base, self.filename_template
            )
            # Vivfy the hook now with the config override
            assert self.oss_task_handler.hook is not None
            # Make sure the connection to oss is set up
        except AirflowException:
            self.hook = None
        except oss2.exceptions.ServerError as e:
            if e.status == 403:
                self.hook = None

    @skip_test_if_no_valid_conn_id
    def test_oss_log_exists(self):
        self.hook.load_string("1.log", "task1 log", TEST_BUCKET)
        assert self.oss_task_handler.oss_log_exists(self.remote_log_location) == True

    @skip_test_if_no_valid_conn_id
    def test_oss_read(self):
        self.hook.load_string("a simple test string", self.remote_log_location, TEST_BUCKET)
        assert self.oss_task_handler.oss_read(self.remote_log_location) == "a simple test string"

    @skip_test_if_no_valid_conn_id
    def test_oss_write(self):
        self.oss_task_handler.oss_write("a new test string", self.remote_log_location, append=False)
        assert self.hook.read_key(TEST_BUCKET, self.remote_log_key) == "a new test string"
