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

from airflow.providers.amazon.aws.links.ec2 import EC2InstanceDashboardLink, EC2InstanceLink
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS

from unit.amazon.aws.links.test_base_aws import BaseAwsLinksTestCase

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult


class TestEC2InstanceLink(BaseAwsLinksTestCase):
    link_class = EC2InstanceLink

    INSTANCE_ID = "i-xxxxxxxxxxxx"

    def test_extra_link(self, mock_supervisor_comms):
        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.get_message.return_value = XComResult(
                key=self.link_class.key,
                value={
                    "region_name": "eu-west-1",
                    "aws_domain": self.link_class.get_aws_domain("aws"),
                    "aws_partition": "aws",
                    "instance_id": self.INSTANCE_ID,
                },
            )
        self.assert_extra_link_url(
            expected_url=(
                "https://console.aws.amazon.com/ec2/home"
                f"?region=eu-west-1#InstanceDetails:instanceId={self.INSTANCE_ID}"
            ),
            region_name="eu-west-1",
            aws_partition="aws",
            instance_id=self.INSTANCE_ID,
        )


class TestEC2InstanceDashboardLink(BaseAwsLinksTestCase):
    link_class = EC2InstanceDashboardLink

    BASE_URL = "https://console.aws.amazon.com/ec2/home"
    INSTANCE_IDS = ["i-xxxxxxxxxxxx", "i-yyyyyyyyyyyy"]

    def test_instance_id_filter(self):
        instance_list = ",:".join(self.INSTANCE_IDS)
        result = EC2InstanceDashboardLink.format_instance_id_filter(self.INSTANCE_IDS)
        assert result == instance_list

    def test_extra_link(self, mock_supervisor_comms):
        instance_list = ",:".join(self.INSTANCE_IDS)
        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.get_message.return_value = XComResult(
                key=self.link_class.key,
                value={
                    "region_name": "eu-west-1",
                    "aws_domain": self.link_class.get_aws_domain("aws"),
                    "aws_partition": "aws",
                    "instance_ids": EC2InstanceDashboardLink.format_instance_id_filter(self.INSTANCE_IDS),
                },
            )
        self.assert_extra_link_url(
            expected_url=(f"{self.BASE_URL}?region=eu-west-1#Instances:instanceId=:{instance_list}"),
            region_name="eu-west-1",
            aws_partition="aws",
            instance_ids=EC2InstanceDashboardLink.format_instance_id_filter(self.INSTANCE_IDS),
        )
