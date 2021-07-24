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
from tests.providers.alibaba.cloud.utils.test_utils import skip_test_if_no_valid_conn_id

TEST_CONN_ID = os.environ.get('TEST_OSS_CONN_ID', 'oss_default')
TEST_REGION = os.environ.get('TEST_OSS_REGION', 'cn-hangzhou')
TEST_BUCKET = os.environ.get('TEST_OSS_BUCKET', 'test-bucket')


class TestOSSHook(unittest.TestCase):
    def setUp(self):
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
        assert self.hook.oss_conn_id == TEST_CONN_ID

    @skip_test_if_no_valid_conn_id
    def test_get_conn(self):
        assert self.hook.get_conn() is not None

    @skip_test_if_no_valid_conn_id
    def test_parse_oss_url(self):
        parsed = self.hook.parse_oss_url(f"oss://{TEST_BUCKET}/this/is/not/a-real-key.txt")
        print(parsed)
        assert parsed == (TEST_BUCKET, "this/is/not/a-real-key.txt"), "Incorrect parsing of the oss url"

    @skip_test_if_no_valid_conn_id
    def test_parse_oss_object_directory(self):
        parsed = self.hook.parse_oss_url(f"oss://{TEST_BUCKET}/this/is/not/a-real-oss-directory/")
        assert parsed == (
            TEST_BUCKET,
            "this/is/not/a-real-oss-directory/",
        ), "Incorrect parsing of the oss url"

    @skip_test_if_no_valid_conn_id
    def test_get_bucket(self):
        assert self.hook.get_bucket(TEST_BUCKET) is not None
