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

from ipaddress import IPv4Network

import boto3

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule


def _get_next_available_cidr(vpc_id: str) -> str:
    """Checks the CIDR blocks of existing subnets and attempts to extrapolate the next available block."""
    error_msg_template = "Can not calculate the next available CIDR block: {}"
    vpc_filter = {"Name": "vpc-id", "Values": [vpc_id]}

    existing_subnets = boto3.client("ec2").describe_subnets(Filters=[vpc_filter])[
        "Subnets"
    ]
    if not existing_subnets:
        raise ValueError(
            error_msg_template.format("No subnets are found on the provided VPC.")
        )

    # Pull all CIDR blocks from the JSON response and convert them
    # to IPv4Network objects which can be sorted and manipulated.
    existing_cidr_blocks = [
        IPv4Network(subnet["CidrBlock"]) for subnet in existing_subnets
    ]
    # Can not predict the next block if existing block sizes (prefixlen) are not consistent.
    if len({block.prefixlen for block in existing_cidr_blocks}) > 1:
        raise ValueError(
            error_msg_template.format("Subnets do not all use the same CIDR block size.")
        )

    last_used_block = max(existing_cidr_blocks)
    *_, last_reserved_ip = last_used_block
    return f"{last_reserved_ip + 1}/{last_used_block.prefixlen}"


@task
def get_default_vpc_id() -> str:
    """Returns the VPC ID of the account's default VPC."""
    filters = [{"Name": "is-default", "Values": ["true"]}]
    return boto3.client("ec2").describe_vpcs(Filters=filters)["Vpcs"][0]["VpcId"]


@task
def create_address_allocation():
    """Allocate a new IP address"""
    return boto3.client("ec2").allocate_address()["AllocationId"]


@task
def create_nat_gateway(allocation_id: str, subnet_id: str):
    """Create a NAT gateway"""
    client = boto3.client("ec2")
    nat_gateway_id = client.create_nat_gateway(
        AllocationId=allocation_id,
        SubnetId=subnet_id,
        ConnectivityType="public",
    )["NatGateway"]["NatGatewayId"]

    waiter = client.get_waiter("nat_gateway_available")
    waiter.wait(NatGatewayIds=[nat_gateway_id])

    return nat_gateway_id


@task
def create_route_table(vpc_id: str, nat_gateway_id: str, test_name: str):
    """Create a route table for private subnets."""
    client = boto3.client("ec2")
    tags = [{"Key": "Name", "Value": f"Route table for {test_name}"}]
    route_table_id = client.create_route_table(
        VpcId=vpc_id,
        TagSpecifications=[{"ResourceType": "route-table", "Tags": tags}],
    )["RouteTable"]["RouteTableId"]

    client.create_route(
        RouteTableId=route_table_id,
        DestinationCidrBlock="0.0.0.0/0",
        NatGatewayId=nat_gateway_id,
    )

    return route_table_id


@task
def create_private_subnets(
    vpc_id: str,
    route_table_id: str,
    test_name: str,
    number_to_make: int = 1,
    cidr_block: str | None = None,
):
    """
    Fargate Profiles require two private subnets in two different availability zones.
    These subnets require as well an egress route to the internet, using a NAT gateway to achieve this.
    """
    client = boto3.client("ec2")
    subnet_ids = []
    tags = [{"Key": "Name", "Value": f"Private Subnet for {test_name}"}]
    zone_names = [
        zone["ZoneName"]
        for zone in client.describe_availability_zones()["AvailabilityZones"]
    ]

    # Create the requested number of subnets.
    for counter in range(number_to_make):
        new_subnet = client.create_subnet(
            VpcId=vpc_id,
            CidrBlock=cidr_block or _get_next_available_cidr(vpc_id),
            AvailabilityZone=zone_names[counter],
            TagSpecifications=[{"ResourceType": "subnet", "Tags": tags}],
        )["Subnet"]["SubnetId"]
        subnet_ids.append(new_subnet)

        # Testing shows that a new subnet takes a very short but measurable
        # time to create; wait to prevent a possible race condition.
        client.get_waiter("subnet_available").wait(SubnetIds=[new_subnet])
        # Associate the new subnets with the black hole route table to make them private.
        client.associate_route_table(RouteTableId=route_table_id, SubnetId=new_subnet)

    return subnet_ids


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_subnets(subnets) -> None:
    for subnet in subnets:
        boto3.client("ec2").delete_subnet(SubnetId=subnet)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_route_table(route_table_id: str) -> None:
    boto3.client("ec2").delete_route_table(RouteTableId=route_table_id)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_nat_gateway(nat_gateway_id: str) -> None:
    client = boto3.client("ec2")
    client.delete_nat_gateway(NatGatewayId=nat_gateway_id)
    waiter = client.get_waiter("nat_gateway_deleted")
    waiter.wait(NatGatewayIds=[nat_gateway_id])


@task(trigger_rule=TriggerRule.ALL_DONE)
def remove_address_allocation(allocation_id):
    boto3.client("ec2").release_address(AllocationId=allocation_id)
