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

import boto3

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule


def create_security_group(sec_group_name: str, ip_permissions: list[dict]):
    client = boto3.client('ec2')
    vpc_id = client.describe_vpcs()['Vpcs'][0]['VpcId']
    security_group = client.create_security_group(
        Description='Redshift-system-test', GroupName=sec_group_name, VpcId=vpc_id
    )
    client.get_waiter('security_group_exists').wait(
        GroupIds=[security_group['GroupId']],
        GroupNames=[sec_group_name],
        WaiterConfig={'Delay': 15, 'MaxAttempts': 4},
    )
    client.authorize_security_group_ingress(
        GroupId=security_group['GroupId'], GroupName=sec_group_name, IpPermissions=ip_permissions
    )
    return security_group


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_security_group(sec_group_id: str, sec_group_name: str):
    boto3.client('ec2').delete_security_group(GroupId=sec_group_id, GroupName=sec_group_name)
