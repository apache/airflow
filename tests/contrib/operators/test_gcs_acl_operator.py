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

from airflow.contrib.operators.gcs_acl_operator import \
    GoogleCloudStorageBucketCreateAclEntryOperator, \
    GoogleCloudStorageObjectCreateAclEntryOperator
from tests.compat import mock


class TestGoogleCloudStorageAcl(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageHook')
    def test_bucket_create_acl(self, mock_hook):
        operator = GoogleCloudStorageBucketCreateAclEntryOperator(
            bucket="test-bucket",
            entity="test-entity",
            role="test-role",
            user_project="test-user-project",
            task_id="id"
        )
        operator.execute(None)
        mock_hook.return_value.insert_bucket_acl.assert_called_once_with(
            bucket_name="test-bucket",
            entity="test-entity",
            role="test-role",
            user_project="test-user-project"
        )

    @mock.patch('airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageHook')
    def test_object_create_acl(self, mock_hook):
        operator = GoogleCloudStorageObjectCreateAclEntryOperator(
            bucket="test-bucket",
            object_name="test-object",
            entity="test-entity",
            generation=42,
            role="test-role",
            user_project="test-user-project",
            task_id="id"
        )
        operator.execute(None)
        mock_hook.return_value.insert_object_acl.assert_called_once_with(
            bucket_name="test-bucket",
            object_name="test-object",
            entity="test-entity",
            generation=42,
            role="test-role",
            user_project="test-user-project"
        )
