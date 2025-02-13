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

from airflow.providers.amazon.aws.links.datasync import DataSyncTaskExecutionLink, DataSyncTaskLink
from provider_tests.amazon.aws.links.test_base_aws import BaseAwsLinksTestCase

TASK_ID = "task-0b36221bf94ad2bdd"
EXECUTION_ID = "exec-00000000000000004"


class TestDataSyncTaskLink(BaseAwsLinksTestCase):
    link_class = DataSyncTaskLink

    def test_extra_link(self):
        task_id = TASK_ID
        self.assert_extra_link_url(
            expected_url=(f"https://console.aws.amazon.com/datasync/home?region=us-east-1#/tasks/{TASK_ID}"),
            region_name="us-east-1",
            aws_partition="aws",
            task_id=task_id,
        )


class TestDataSyncTaskExecutionLink(BaseAwsLinksTestCase):
    link_class = DataSyncTaskExecutionLink

    def test_extra_link(self):
        self.assert_extra_link_url(
            expected_url=(
                f"https://console.aws.amazon.com/datasync/home?region=us-east-1#/history/{TASK_ID}/{EXECUTION_ID}"
            ),
            region_name="us-east-1",
            aws_partition="aws",
            task_id=TASK_ID,
            task_execution_id=EXECUTION_ID,
        )
