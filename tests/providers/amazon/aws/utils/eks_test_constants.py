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

"""
This file should only contain constants used for the EKS tests.
"""
import re
from enum import Enum
from typing import Dict, List, Pattern, Tuple

DEFAULT_CONN_ID: str = "aws_default"
FROZEN_TIME: str = "2013-11-27T01:42:00Z"
PACKAGE_NOT_PRESENT_MSG: str = "mock_eks package not present"
PARTITION: str = "aws"
NON_EXISTING_CLUSTER_NAME: str = "non_existing_cluster"
NON_EXISTING_NODEGROUP_NAME: str = "non_existing_nodegroup"
REGION: str = "us-east-1"
SUBNET_IDS: List[str] = ["subnet-12345ab", "subnet-67890cd"]
TASK_ID: str = "test-eks-operator"


AMI_TYPE: Tuple[str, str] = ("amiType", "AL2_x86_64")
CLIENT_REQUEST_TOKEN: Tuple[str, str] = ("clientRequestToken", "test_request_token")
DISK_SIZE: Tuple[str, int] = ("diskSize", 30)
ENCRYPTION_CONFIG: Tuple[str, List] = (
    "encryptionConfig",
    [{"resources": ["secrets"], "provider": {"keyArn": "arn:of:the:key"}}],
)
INSTANCE_TYPES: Tuple[str, List] = ("instanceTypes", ["t3.medium"])
KUBERNETES_NETWORK_CONFIG: Tuple[str, Dict] = (
    "kubernetesNetworkConfig",
    {"serviceIpv4Cidr": "172.20.0.0/16"},
)
LABELS: Tuple[str, Dict] = ("labels", {"purpose": "example"})
LAUNCH_TEMPLATE: Tuple[str, Dict] = ("launchTemplate", {"name": "myTemplate", "version": "2", "id": "123456"})
LOGGING: Tuple[str, Dict] = ("logging", {"clusterLogging": [{"types": ["api"], "enabled": True}]})
NODEROLE_ARN: Tuple[str, str] = ("nodeRole", "arn:aws:iam::123456789012:role/role_name")
REMOTE_ACCESS: Tuple[str, Dict] = ("remoteAccess", {"ec2SshKey": "eksKeypair"})
RESOURCES_VPC_CONFIG: Tuple[str, Dict] = (
    "resourcesVpcConfig",
    {
        "subnetIds": SUBNET_IDS,
        "endpointPublicAccess": True,
        "endpointPrivateAccess": False,
    },
)
ROLE_ARN: Tuple[str, str] = ("roleArn", "arn:aws:iam::123456789012:role/role_name")
SCALING_CONFIG: Tuple[str, Dict] = ("scalingConfig", {"minSize": 2, "maxSize": 3, "desiredSize": 2})
STATUS: Tuple[str, str] = ("status", "ACTIVE")
SUBNETS: Tuple[str, List] = ("subnets", SUBNET_IDS)
TAGS: Tuple[str, Dict] = ("tags", {"hello": "world"})
VERSION: Tuple[str, str] = ("version", "1")


class ResponseAttributes:
    """Key names for the dictionaries returned by API calls."""

    CLUSTER: slice = "cluster"
    CLUSTERS: slice = "clusters"
    NEXT_TOKEN: slice = "nextToken"
    NODEGROUP: slice = "nodegroup"
    NODEGROUPS: slice = "nodegroups"


class ErrorAttributes:
    """Key names for the dictionaries representing error messages."""

    CODE: slice = "Code"
    ERROR: slice = "Error"
    MESSAGE: slice = "Message"


class ClusterInputs:
    """All possible inputs for creating an EKS Cluster."""

    REQUIRED: List[Tuple] = [ROLE_ARN, RESOURCES_VPC_CONFIG]
    OPTIONAL: List[Tuple] = [
        CLIENT_REQUEST_TOKEN,
        ENCRYPTION_CONFIG,
        LOGGING,
        KUBERNETES_NETWORK_CONFIG,
        TAGS,
        VERSION,
    ]


class NodegroupInputs:
    """All possible inputs for creating an EKS Managed Nodegroup."""

    REQUIRED: List[Tuple] = [NODEROLE_ARN, SUBNETS]
    OPTIONAL: List[Tuple] = [
        AMI_TYPE,
        DISK_SIZE,
        INSTANCE_TYPES,
        LABELS,
        REMOTE_ACCESS,
        SCALING_CONFIG,
        TAGS,
    ]


class PossibleTestResults(Enum):
    """Possible test results."""

    SUCCESS: str = "SUCCESS"
    FAILURE: str = "FAILURE"


class ClusterAttributes:
    """Key names for the dictionaries representing EKS Clusters."""

    ARN: slice = "arn"
    CLUSTER_NAME: slice = "clusterName"
    CREATED_AT: slice = "createdAt"
    ENDPOINT: slice = "endpoint"
    IDENTITY: slice = "identity"
    ISSUER: slice = "issuer"
    NAME: slice = "name"
    OIDC: slice = "oidc"


class NodegroupAttributes:
    """Key names for the dictionaries representing EKS Managed Nodegroups."""

    ARN: slice = "nodegroupArn"
    AUTOSCALING_GROUPS: slice = "autoScalingGroups"
    CREATED_AT: slice = "createdAt"
    MODIFIED_AT: slice = "modifiedAt"
    NAME: slice = "name"
    NODEGROUP_NAME: slice = "nodegroupName"
    REMOTE_ACCESS_SG: slice = "remoteAccessSecurityGroup"
    RESOURCES: slice = "resources"
    TAGS: slice = "tags"


class BatchCountSize:
    """Sizes of test data batches to generate."""

    SINGLE: int = 1
    SMALL: int = 10
    MEDIUM: int = 20
    LARGE: int = 200


class PageCount:
    """Page lengths to use when testing pagination."""

    SMALL: int = 3
    LARGE: int = 10


NODEGROUP_UUID_PATTERN: str = (
    "(?P<nodegroup_uuid>[-0-9a-z]{8}-[-0-9a-z]{4}-[-0-9a-z]{4}-[-0-9a-z]{4}-[-0-9a-z]{12})"
)


class RegExTemplates:
    """The compiled RegEx patterns used in testing."""

    CLUSTER_ARN: Pattern = re.compile(
        "arn:"
        + "(?P<partition>.+):"
        + "eks:"
        + "(?P<region>[-0-9a-zA-Z]+):"
        + "(?P<account_id>[0-9]{12}):"
        + "cluster/"
        + "(?P<cluster_name>.+)"
    )
    NODEGROUP_ARN: Pattern = re.compile(
        "arn:"
        + "(?P<partition>.+):"
        + "eks:"
        + "(?P<region>[-0-9a-zA-Z]+):"
        + "(?P<account_id>[0-9]{12}):"
        + "nodegroup/"
        + "(?P<cluster_name>.+)/"
        + "(?P<nodegroup_name>.+)/"
        + NODEGROUP_UUID_PATTERN
    )
    NODEGROUP_ASG_NAME_PATTERN: Pattern = re.compile("eks-" + NODEGROUP_UUID_PATTERN)
    NODEGROUP_SECURITY_GROUP_NAME_PATTERN: Pattern = re.compile("sg-" + "([-0-9a-z]{17})")


class MethodNames:
    """The names of methods, used when a test is expected to throw an exception."""

    CREATE_CLUSTER: str = "CreateCluster"
    CREATE_NODEGROUP: str = "CreateNodegroup"
    DELETE_CLUSTER: str = "DeleteCluster"
    DELETE_NODEGROUP: str = "DeleteNodegroup"
    DESCRIBE_CLUSTER: str = "DescribeCluster"
    DESCRIBE_NODEGROUP: str = "DescribeNodegroup"
