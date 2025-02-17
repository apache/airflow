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

from airflow.providers.amazon.aws.links.comprehend import (
    ComprehendDocumentClassifierLink,
    ComprehendPiiEntitiesDetectionLink,
)
from unit.amazon.aws.links.test_base_aws import BaseAwsLinksTestCase


class TestComprehendPiiEntitiesDetectionLink(BaseAwsLinksTestCase):
    link_class = ComprehendPiiEntitiesDetectionLink

    def test_extra_link(self):
        test_job_id = "123-345-678"
        self.assert_extra_link_url(
            expected_url=(
                f"https://console.aws.amazon.com/comprehend/home?region=eu-west-1#/analysis-job-details/pii/{test_job_id}"
            ),
            region_name="eu-west-1",
            aws_partition="aws",
            job_id=test_job_id,
        )


class TestComprehendDocumentClassifierLink(BaseAwsLinksTestCase):
    link_class = ComprehendDocumentClassifierLink

    def test_extra_link(self):
        test_job_id = (
            "arn:aws:comprehend:us-east-1:0123456789:document-classifier/test-custom-document-classifier"
        )
        self.assert_extra_link_url(
            expected_url=(
                f"https://console.aws.amazon.com/comprehend/home?region=us-east-1#classifier-version-details/{test_job_id}"
            ),
            region_name="us-east-1",
            aws_partition="aws",
            arn=test_job_id,
        )
