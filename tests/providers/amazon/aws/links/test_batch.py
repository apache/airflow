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

from airflow.providers.amazon.aws.links.batch import (
    BatchJobDefinitionLink,
    BatchJobDetailsLink,
    BatchJobQueueLink,
)
from tests.providers.amazon.aws.links.test_base_aws import BaseAwsLinksTestCase


class TestBatchJobDefinitionLink(BaseAwsLinksTestCase):
    link_class = BatchJobDefinitionLink

    def test_extra_link(self):
        self.assert_extra_link_url(
            expected_url=(
                "https://console.aws.amazon.com/batch/home"
                "?region=eu-west-1#job-definition/detail/arn:fake:jd"
            ),
            region_name="eu-west-1",
            aws_partition="aws",
            job_definition_arn="arn:fake:jd",
        )


class TestBatchJobDetailsLink(BaseAwsLinksTestCase):
    link_class = BatchJobDetailsLink

    def test_extra_link(self):
        self.assert_extra_link_url(
            expected_url="https://console.amazonaws.cn/batch/home?region=cn-north-1#jobs/detail/fake-id",
            region_name="cn-north-1",
            aws_partition="aws-cn",
            job_id="fake-id",
        )


class TestBatchJobQueueLink(BaseAwsLinksTestCase):
    link_class = BatchJobQueueLink

    def test_extra_link(self):
        self.assert_extra_link_url(
            expected_url=(
                "https://console.aws.amazon.com/batch/home" "?region=us-east-1#queues/detail/arn:fake:jq"
            ),
            region_name="us-east-1",
            aws_partition="aws",
            job_queue_arn="arn:fake:jq",
        )
