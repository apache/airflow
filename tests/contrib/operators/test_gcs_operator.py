# -*- coding: utf-8 -*-
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

import unittest

from airflow.contrib.operators.gcs_operator import GoogleCloudStorageCreateBucketOperator
from airflow.version import version
from tests.compat import mock

TASK_ID = 'test-gcs-operator'
TEST_BUCKET = 'test-bucket'
TEST_PROJECT = 'test-project'


class GoogleCloudStorageCreateBucketTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcs_operator.GoogleCloudStorageHook')
    def test_execute(self, mock_hook):
        operator = GoogleCloudStorageCreateBucketOperator(
            task_id=TASK_ID,
            bucket_name=TEST_BUCKET,
            resource={"lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7}}]}},
            storage_class='MULTI_REGIONAL',
            location='EU',
            labels={'env': 'prod'},
            project_id=TEST_PROJECT
        )

        operator.execute(None)
        mock_hook.return_value.create_bucket.assert_called_once_with(
            bucket_name=TEST_BUCKET, storage_class='MULTI_REGIONAL',
            location='EU', labels={
                'airflow-version': 'v' + version.replace('.', '-').replace('+', '-'),
                'env': 'prod'
            }, project_id=TEST_PROJECT,
            resource={'lifecycle': {'rule': [{'action': {'type': 'Delete'}, 'condition': {'age': 7}}]}}
        )
