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

import datetime
import re
from copy import deepcopy
from typing import Pattern, Type, Union

from airflow.providers.amazon.aws.hooks.eks import EksHook

from ..utils.eks_test_constants import (
    STATUS,
    ClusterAttributes,
    ClusterInputs,
    FargateProfileAttributes,
    FargateProfileInputs,
    NodegroupAttributes,
    NodegroupInputs,
    ResponseAttributes,
)

InputTypes = Union[Type[ClusterInputs], Type[NodegroupInputs], Type[FargateProfileInputs]]


def attributes_to_test(
    inputs: InputTypes,
    cluster_name: str,
    fargate_profile_name: str | None = None,
    nodegroup_name: str | None = None,
) -> list[tuple]:
    """
    Assembles the list of tuples which will be used to validate test results.
    The format of the tuple is (attribute name, expected value)

    :param inputs: A class containing lists of tuples to use for verifying the output
    of cluster or nodegroup creation tests.
    :param cluster_name: The name of the cluster under test.
    :param fargate_profile_name: The name of the Fargate profile under test if applicable.
    :param nodegroup_name: The name of the nodegroup under test if applicable.
    :return: Returns a list of tuples containing the keys and values to be validated in testing.
    """
    result: list[tuple] = deepcopy(inputs.REQUIRED + inputs.OPTIONAL + [STATUS])  # type: ignore
    if inputs == ClusterInputs:
        result += [(ClusterAttributes.NAME, cluster_name)]
    elif inputs == FargateProfileInputs:
        result += [(FargateProfileAttributes.FARGATE_PROFILE_NAME, fargate_profile_name)]
    elif inputs == NodegroupInputs:
        # The below tag is mandatory and must have a value of either 'owned' or 'shared'
        # A value of 'owned' denotes that the subnets are exclusive to the nodegroup.
        # The 'shared' value allows more than one resource to use the subnet.
        required_tag: dict = {f"kubernetes.io/cluster/{cluster_name}": "owned"}
        # Find the user-submitted tag set and append the required tag to it.
        final_tag_set: dict = required_tag
        for key, value in result:
            if key == "tags":
                final_tag_set = {**value, **final_tag_set}
        # Inject it back into the list.
        result = [
            (key, value) if (key != NodegroupAttributes.TAGS) else (NodegroupAttributes.TAGS, final_tag_set)
            for key, value in result
        ]

        result += [(NodegroupAttributes.NODEGROUP_NAME, nodegroup_name)]

    return result


def generate_clusters(eks_hook: EksHook, num_clusters: int, minimal: bool) -> list[str]:
    """
    Generates a number of EKS Clusters with data and adds them to the mocked backend.

    :param eks_hook: An EksHook object used to call the EKS API.
    :param num_clusters: Number of clusters to generate.
    :param minimal: If True, only the required values are generated; if False all values are generated.
    :return: Returns a list of the names of the generated clusters.
    """
    # Generates N clusters named cluster0, cluster1, .., clusterN
    return [
        eks_hook.create_cluster(name=f"cluster{str(count)}", **_input_builder(ClusterInputs, minimal))[
            ResponseAttributes.CLUSTER
        ][ClusterAttributes.NAME]
        for count in range(num_clusters)
    ]


def generate_fargate_profiles(
    eks_hook: EksHook, cluster_name: str, num_profiles: int, minimal: bool
) -> list[str]:
    """
    Generates a number of EKS Fargate profiles with data and adds them to the mocked backend.

    :param eks_hook: An EksHook object used to call the EKS API.
    :param cluster_name: The name of the EKS Cluster to attach the nodegroups to.
    :param num_profiles: Number of Fargate profiles to generate.
    :param minimal: If True, only the required values are generated; if False all values are generated.
    :return: Returns a list of the names of the generated nodegroups.
    """
    # Generates N Fargate profiles named profile0, profile1, .., profileN
    return [
        eks_hook.create_fargate_profile(
            fargateProfileName=f"profile{str(count)}",
            clusterName=cluster_name,
            **_input_builder(FargateProfileInputs, minimal),
        )[ResponseAttributes.FARGATE_PROFILE][FargateProfileAttributes.FARGATE_PROFILE_NAME]
        for count in range(num_profiles)
    ]


def generate_nodegroups(
    eks_hook: EksHook, cluster_name: str, num_nodegroups: int, minimal: bool
) -> list[str]:
    """
    Generates a number of EKS Managed Nodegroups with data and adds them to the mocked backend.

    :param eks_hook: An EksHook object used to call the EKS API.
    :param cluster_name: The name of the EKS Cluster to attach the nodegroups to.
    :param num_nodegroups: Number of clusters to generate.
    :param minimal: If True, only the required values are generated; if False all values are generated.
    :return: Returns a list of the names of the generated nodegroups.
    """
    # Generates N nodegroups named nodegroup0, nodegroup1, .., nodegroupN
    return [
        eks_hook.create_nodegroup(
            nodegroupName=f"nodegroup{str(count)}",
            clusterName=cluster_name,
            **_input_builder(NodegroupInputs, minimal),
        )[ResponseAttributes.NODEGROUP][NodegroupAttributes.NODEGROUP_NAME]
        for count in range(num_nodegroups)
    ]


def region_matches_partition(region: str, partition: str) -> bool:
    """
    Returns True if the provided region and partition are a valid pair.

    :param region: AWS region code to test.
    :param partition: AWS partition code to test.
    :return: Returns True if the provided region and partition are a valid pair.
    """
    valid_matches: list[tuple[str, str]] = [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-gov-iso-", "aws-iso"),
        ("us-gov-iso-b-", "aws-iso-b"),
    ]

    for prefix, expected_partition in valid_matches:
        if region.startswith(prefix):
            return partition == expected_partition
    return partition == "aws"


def _input_builder(options: InputTypes, minimal: bool) -> dict:
    """
    Assembles the inputs which will be used to generate test object into a dictionary.

    :param options: A class containing lists of tuples to use for to create
    the cluster or nodegroup used in testing.
    :param minimal: If True, only the required values are generated; if False all values are generated.
    :return: Returns a dict containing the keys and values to be validated in testing.
    """
    values: list[tuple] = deepcopy(options.REQUIRED)  # type: ignore
    if not minimal:
        values.extend(deepcopy(options.OPTIONAL))
    return dict(values)  # type: ignore


def string_to_regex(value: str) -> Pattern[str]:
    """
    Converts a string template into a regex template for pattern matching.

    :param value: The template string to convert.
    :returns: Returns a regex pattern
    """
    return re.compile(re.sub(r"[{](.*?)[}]", r"(?P<\1>.+)", value))


def convert_keys(original: dict) -> dict:
    """
    API Input and Output keys are formatted differently.  The EKS Hooks map
    as closely as possible to the API calls, which use camelCase variable
    names, but the Operators match python conventions and use snake_case.
    This method converts the keys of a dict which are in snake_case (input
    format) to camelCase (output format) while leaving the dict values unchanged.

    :param original: Dict which needs the keys converted.
    :value original: Dict
    """
    if "nodegroup_name" in original.keys():
        conversion_map = {
            "cluster_name": "clusterName",
            "cluster_role_arn": "roleArn",
            "nodegroup_subnets": "subnets",
            "subnets": "subnets",
            "nodegroup_name": "nodegroupName",
            "nodegroup_role_arn": "nodeRole",
        }
    elif "fargate_profile_name" in original.keys():
        conversion_map = {
            "cluster_name": "clusterName",
            "fargate_profile_name": "fargateProfileName",
            "subnets": "subnets",
            # The following are "duplicated" because we used the more verbose/descriptive version
            # in the CreateCluster Operator when creating a cluster alongside a Fargate profile, but
            # the more terse version in the CreateFargateProfile Operator for the sake of convenience.
            "pod_execution_role_arn": "podExecutionRoleArn",
            "fargate_pod_execution_role_arn": "podExecutionRoleArn",
            "selectors": "selectors",
            "fargate_selectors": "selectors",
        }
    else:
        conversion_map = {
            "cluster_name": "name",
            "cluster_role_arn": "roleArn",
            "resources_vpc_config": "resourcesVpcConfig",
        }

    return {conversion_map[k] if k in conversion_map else k: v for (k, v) in deepcopy(original).items()}


def iso_date(input_datetime: datetime.datetime) -> str:
    return input_datetime.strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def generate_dict(prefix, count) -> dict:
    return {f"{prefix}_{_count}": str(_count) for _count in range(count)}
