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

from airflow.providers.amazon.aws.links.base_aws import BaseAwsLink
from airflow.providers.amazon.aws.links.sagemaker import SageMakerTransformJobLink
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS

from unit.amazon.aws.links.test_base_aws import BaseAwsLinksTestCase

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult


class TestSageMakerTransformDetailsLink(BaseAwsLinksTestCase):
    link_class = SageMakerTransformJobLink

    def test_extra_link(self, mock_supervisor_comms):
        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="sagemaker_transform_job_details",
                value={
                    "region_name": "us-east-1",
                    "aws_domain": BaseAwsLink.get_aws_domain("aws"),
                    **{"job_name": "test_job_name"},
                },
            )

        self.assert_extra_link_url(
            expected_url=(
                "https://console.aws.amazon.com/sagemaker/home?region=us-east-1#/transform-jobs/test_job_name"
            ),
            region_name="us-east-1",
            aws_partition="aws",
            job_name="test_job_name",
        )
