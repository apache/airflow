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

import boto3
from botocore.exceptions import ClientError

from airflow.decorators import task


@task
def create_iam_role(
    role_name: str, trust_policy: str, role_description: str, policy_arn: str, instance_profile_name: str
):
    client = boto3.client("iam")

    try:
        client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description=role_description,
        )

    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise

    client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

    try:
        client.create_instance_profile(InstanceProfileName=instance_profile_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise

    instance_profile = client.get_instance_profile(InstanceProfileName=instance_profile_name)

    attached_roles = [role["RoleName"] for role in instance_profile["InstanceProfile"]["Roles"]]
    if role_name not in attached_roles:
        client.add_role_to_instance_profile(InstanceProfileName=instance_profile_name, RoleName=role_name)
