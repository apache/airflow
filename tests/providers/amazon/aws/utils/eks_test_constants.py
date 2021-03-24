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
import os
import re
from enum import Enum
from typing import Dict, List, Pattern, Tuple

from boto3 import Session

CONN_ID = "eks"
DEFAULT_MAX_RESULTS = 100
FROZEN_TIME = "2013-11-27T01:42:00Z"
PACKAGE_NOT_PRESENT_MSG = "mock_eks package not present"
PARTITIONS: List[str] = Session().get_available_partitions()
REGION: str = Session().region_name
SUBNET_IDS: List[str] = ["subnet-12345ab", "subnet-67890cd"]
TASK_ID = os.environ.get("TASK_ID", "test-eks-operator")


AMI_TYPE_KEY: str = "amiType"
AMI_TYPE_VALUE: str = "AL2_x86_64"

CLIENT_REQUEST_TOKEN_KEY: str = "clientRequestToken"
CLIENT_REQUEST_TOKEN_VALUE: str = "test_request_token"

DISK_SIZE_KEY: str = "diskSize"
DISK_SIZE_VALUE: int = 30

ENCRYPTION_CONFIG_KEY: str = "encryptionConfig"
ENCRYPTION_CONFIG_VALUE: List[Dict] = [{"resources": ["secrets"], "provider": {"keyArn": "arn:of:the:key"}}]

INSTANCE_TYPES_KEY: str = "instanceTypes"
INSTANCE_TYPES_VALUE: List[str] = ["t3.medium"]

KUBERNETES_NETWORK_CONFIG_KEY: str = "kubernetesNetworkConfig"
KUBERNETES_NETWORK_CONFIG_VALUE: Dict = {"serviceIpv4Cidr": "172.20.0.0/16"}

LABELS_KEY: str = "labels"
LABELS_VALUE: Dict = {"purpose": "example"}

LAUNCH_TEMPLATE_KEY: str = "launchTemplate"
LAUNCH_TEMPLATE_VALUE: Dict = {"name": "myTemplate", "version": "2", "id": "123456"}

LOGGING_KEY: str = "logging"
LOGGING_VALUE: Dict = {"clusterLogging": [{"types": ["api"], "enabled": True}]}

NODEROLE_ARN_KEY: str = "nodeRole"
NODEROLE_ARN_VALUE: str = "arn:aws:iam::123456789012:role/role_name"

REMOTE_ACCESS_KEY: str = "remoteAccess"
REMOTE_ACCESS_VALUE: Dict = {"ec2SshKey": "eksKeypair"}

RESOURCES_VPC_CONFIG_KEY: str = "resourcesVpcConfig"
RESOURCES_VPC_CONFIG_VALUE: Dict = {
    "subnetIds": SUBNET_IDS,
    "endpointPublicAccess": True,
    "endpointPrivateAccess": False,
}

ROLE_ARN_KEY: str = "roleArn"
ROLE_ARN_VALUE: str = "arn:aws:iam::123456789012:role/role_name"

SCALING_CONFIG_KEY: str = "scalingConfig"
SCALING_CONFIG_VALUE: Dict = {"minSize": 2, "maxSize": 3, "desiredSize": 2}

STATUS_KEY: str = "status"
STATUS_VALUE: str = "ACTIVE"

SUBNETS_KEY: str = "subnets"
SUBNETS_VALUE: List[str] = SUBNET_IDS

TAGS_KEY: str = "tags"
TAGS_VALUE: Dict = {"hello": "world"}

VERSION_KEY: str = "version"
VERSION_VALUE: str = "1"

AMI_TYPE: Tuple = (AMI_TYPE_KEY, AMI_TYPE_VALUE)
CLIENT_REQUEST_TOKEN: Tuple = (CLIENT_REQUEST_TOKEN_KEY, CLIENT_REQUEST_TOKEN_VALUE)
DISK_SIZE: Tuple = (DISK_SIZE_KEY, DISK_SIZE_VALUE)
ENCRYPTION_CONFIG: Tuple = (ENCRYPTION_CONFIG_KEY, ENCRYPTION_CONFIG_VALUE)
INSTANCE_TYPES: Tuple = (INSTANCE_TYPES_KEY, INSTANCE_TYPES_VALUE)
KUBERNETES_NETWORK_CONFIG: Tuple = (
    KUBERNETES_NETWORK_CONFIG_KEY,
    KUBERNETES_NETWORK_CONFIG_VALUE,
)
LABELS: Tuple = (LABELS_KEY, LABELS_VALUE)
LAUNCH_TEMPLATE: Tuple = (LAUNCH_TEMPLATE_KEY, LAUNCH_TEMPLATE_VALUE)
LOGGING: Tuple = (LOGGING_KEY, LOGGING_VALUE)
NODEROLE_ARN: Tuple = (NODEROLE_ARN_KEY, NODEROLE_ARN_VALUE)
REMOTE_ACCESS: Tuple = (REMOTE_ACCESS_KEY, REMOTE_ACCESS_VALUE)
RESOURCES_VPC_CONFIG: Tuple = (RESOURCES_VPC_CONFIG_KEY, RESOURCES_VPC_CONFIG_VALUE)
ROLE_ARN: Tuple = (ROLE_ARN_KEY, ROLE_ARN_VALUE)
SCALING_CONFIG: Tuple = (SCALING_CONFIG_KEY, SCALING_CONFIG_VALUE)
STATUS: Tuple = (STATUS_KEY, STATUS_VALUE)
SUBNETS: Tuple = (SUBNETS_KEY, SUBNETS_VALUE)
TAGS: Tuple = (TAGS_KEY, TAGS_VALUE)
VERSION: Tuple = (VERSION_KEY, VERSION_VALUE)


class ResponseAttribute:
    """Key names for the dictionaries returned by API calls."""

    CLUSTER: slice = "cluster"
    CLUSTERS: slice = "clusters"
    NEXT_TOKEN: slice = "nextToken"
    NODEGROUP: slice = "nodegroup"
    NODEGROUPS: slice = "nodegroups"


class ErrorAttributes:
    CODE = "Code"
    ERROR = "Error"
    MESSAGE = "Message"


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
