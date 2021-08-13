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
#
import re
from copy import deepcopy
from typing import Dict, List, Optional, Pattern, Tuple, Type, Union

from airflow.providers.amazon.aws.hooks.eks import EKSHook

from ..utils.eks_test_constants import (
    STATUS,
    ClusterAttributes,
    ClusterInputs,
    NodegroupAttributes,
    NodegroupInputs,
    ResponseAttributes,
)


def attributes_to_test(
    inputs: Union[Type[ClusterInputs], Type[NodegroupInputs]],
    cluster_name: str,
    nodegroup_name: Optional[str] = None,
) -> List[Tuple]:
    """
    Assembles the list of tuples which will be used to validate test results.
    The format of the tuple is (attribute name, expected value)

    :param inputs: A class containing lists of tuples to use for verifying the output
    of cluster or nodegroup creation tests.
    :type inputs: Union[ClusterInputs, NodegroupInputs]
    :param cluster_name: The name of the cluster under test.
    :type cluster_name: str
    :param nodegroup_name: The name of the nodegroup under test if applicable.
    :type nodegroup_name: str
    :return: Returns a list of tuples containing the keys and values to be validated in testing.
    :rtype: List[Tuple]
    """
    result: List[Tuple] = deepcopy(inputs.REQUIRED + inputs.OPTIONAL + [STATUS])
    if inputs == ClusterInputs:
        result += [(ClusterAttributes.NAME, cluster_name)]
    elif inputs == NodegroupInputs:
        # The below tag is mandatory and must have a value of either 'owned' or 'shared'
        # A value of 'owned' denotes that the subnets are exclusive to the nodegroup.
        # The 'shared' value allows more than one resource to use the subnet.
        required_tag: Dict = {'kubernetes.io/cluster/' + cluster_name: 'owned'}
        # Find the user-submitted tag set and append the required tag to it.
        final_tag_set: Dict = required_tag
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


def generate_clusters(eks_hook: EKSHook, num_clusters: int, minimal: bool) -> List[str]:
    """
    Generates a number of EKS Clusters with randomized data and adds them to the mocked backend.

    :param eks_hook: An EKSHook object used to call the EKS API.
    :type eks_hook: EKSHook
    :param num_clusters: Number of clusters to generate.
    :type num_clusters: int
    :param minimal: If True, only the required values are generated; if False all values are generated.
    :type minimal: bool
    :return: Returns a list of the names of the generated clusters.
    :rtype: List[str]
    """
    # Generates N clusters named cluster0, cluster1, .., clusterN
    return [
        eks_hook.create_cluster(name="cluster" + str(count), **_input_builder(ClusterInputs, minimal))[
            ResponseAttributes.CLUSTER
        ][ClusterAttributes.NAME]
        for count in range(num_clusters)
    ]


def generate_nodegroups(
    eks_hook: EKSHook, cluster_name: str, num_nodegroups: int, minimal: bool
) -> List[str]:
    """
    Generates a number of EKS Managed Nodegroups with randomized data and adds them to the mocked backend.

    :param eks_hook: An EKSHook object used to call the EKS API.
    :type eks_hook: EKSHook
    :param cluster_name: The name of the EKS Cluster to attach the nodegroups to.
    :type cluster_name: str
    :param num_nodegroups: Number of clusters to generate.
    :type num_nodegroups: int
    :param minimal: If True, only the required values are generated; if False all values are generated.
    :type minimal: bool
    :return: Returns a list of the names of the generated nodegroups.
    :rtype: List[str]
    """
    # Generates N nodegroups named nodegroup0, nodegroup1, .., nodegroupN
    return [
        eks_hook.create_nodegroup(
            nodegroupName="nodegroup" + str(count),
            clusterName=cluster_name,
            **_input_builder(NodegroupInputs, minimal),
        )[ResponseAttributes.NODEGROUP][NodegroupAttributes.NODEGROUP_NAME]
        for count in range(num_nodegroups)
    ]


def region_matches_partition(region: str, partition: str) -> bool:
    """
    Returns True if the provided region and partition are a valid pair.

    :param region: AWS region code to test.
    :type: region: str
    :param partition: AWS partition code to test.
    :type partition: str
    :return: Returns True if the provided region and partition are a valid pair.
    :rtype: bool
    """
    valid_matches: List[Tuple[str, str]] = [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-gov-iso-", "aws-iso"),
        ("us-gov-iso-b-", "aws-iso-b"),
    ]

    for prefix, expected_partition in valid_matches:
        if region.startswith(prefix):
            return partition == expected_partition
    return partition == "aws"


def _input_builder(options: Union[Type[ClusterInputs], Type[NodegroupInputs]], minimal: bool) -> Dict:
    """
    Assembles the inputs which will be used to generate test object into a dictionary.

    :param options: A class containing lists of tuples to use for to create
    the cluster or nodegroup used in testing.
    :type options: Union[ClusterInputs, NodegroupInputs]
    :param minimal: If True, only the required values are generated; if False all values are generated.
    :type minimal: bool
    :return: Returns a list of tuples containing the keys and values to be validated in testing.
    :rtype: List[Tuple]
    """
    values: List[Tuple] = deepcopy(options.REQUIRED)
    if not minimal:
        values.extend(deepcopy(options.OPTIONAL))
    return dict(values)


def string_to_regex(value: str) -> Pattern[str]:
    """
    Converts a string template into a regex template for pattern matching.

    :param value: The template string to convert.
    :type value: str
    :returns: Returns a regex pattern
    :rtype: Pattern[str]
    """
    return re.compile(re.sub(r"[{](.*?)[}]", r"(?P<\1>.+)", value))


def convert_keys(original: Dict) -> Dict:
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
        conversion_map = dict(
            cluster_name="clusterName",
            cluster_role_arn="roleArn",
            nodegroup_subnets="subnets",
            subnets="subnets",
            nodegroup_name="nodegroupName",
            nodegroup_role_arn="nodeRole",
        )
    else:
        conversion_map = dict(
            cluster_name="name",
            cluster_role_arn="roleArn",
            resources_vpc_config="resourcesVpcConfig",
        )

    return {conversion_map[k]: v for (k, v) in deepcopy(original).items()}


def iso_date(datetime: str) -> str:
    return datetime.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
