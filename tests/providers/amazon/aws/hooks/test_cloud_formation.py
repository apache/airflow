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
from __future__ import annotations

import json

from moto import mock_cloudformation

from airflow.providers.amazon.aws.hooks.cloud_formation import CloudFormationHook


class TestCloudFormationHook:
    def setup_method(self):
        self.hook = CloudFormationHook(aws_conn_id="aws_default")

    def create_stack(self, stack_name):
        timeout = 15
        template_body = json.dumps(
            {
                "Resources": {
                    "myResource": {
                        "Type": "AWS::EC2::VPC",
                        "Properties": {
                            "CidrBlock": {"Ref": "VPCCidr"},
                            "Tags": [{"Key": "Name", "Value": "Primary_CF_VPC"}],
                        },
                    }
                },
                "Parameters": {
                    "VPCCidr": {
                        "Type": "String",
                        "Default": "10.0.0.0/16",
                        "Description": "Enter the CIDR block for the VPC. Default is 10.0.0.0/16.",
                    }
                },
            }
        )

        self.hook.create_stack(
            stack_name=stack_name,
            cloudformation_parameters={
                "TimeoutInMinutes": timeout,
                "TemplateBody": template_body,
                "Parameters": [{"ParameterKey": "VPCCidr", "ParameterValue": "10.0.0.0/16"}],
            },
        )

    @mock_cloudformation
    def test_get_conn_returns_a_boto3_connection(self):
        assert self.hook.get_conn().describe_stacks() is not None

    @mock_cloudformation
    def test_get_stack_status(self):
        stack_name = "my_test_get_stack_status_stack"

        stack_status = self.hook.get_stack_status(stack_name=stack_name)
        assert stack_status is None

        self.create_stack(stack_name)
        stack_status = self.hook.get_stack_status(stack_name=stack_name)
        assert stack_status == "CREATE_COMPLETE", "Incorrect stack status returned."

    @mock_cloudformation
    def test_create_stack(self):
        stack_name = "my_test_create_stack_stack"
        self.create_stack(stack_name)

        stacks = self.hook.get_conn().describe_stacks()["Stacks"]
        assert len(stacks) > 0, "CloudFormation should have stacks"

        matching_stacks = [x for x in stacks if x["StackName"] == stack_name]
        assert len(matching_stacks) == 1, f"stack with name {stack_name} should exist"

        stack = matching_stacks[0]
        assert stack["StackStatus"] == "CREATE_COMPLETE", "Stack should be in status CREATE_COMPLETE"

    @mock_cloudformation
    def test_delete_stack(self):
        stack_name = "my_test_delete_stack_stack"
        self.create_stack(stack_name)

        self.hook.delete_stack(stack_name=stack_name)

        stacks = self.hook.get_conn().describe_stacks()["Stacks"]
        matching_stacks = [x for x in stacks if x["StackName"] == stack_name]
        assert not matching_stacks, f"stack with name {stack_name} should not exist"
