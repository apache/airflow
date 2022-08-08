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

"""Interact with Amazon EKS, using the boto3 library."""
import base64
import json
import sys
import tempfile
import warnings
from contextlib import contextmanager
from enum import Enum
from functools import partial
from typing import Callable, Dict, Generator, List, Optional

from botocore.exceptions import ClientError
from botocore.signers import RequestSigner

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils import yaml
from airflow.utils.json import AirflowJsonEncoder

DEFAULT_PAGINATION_TOKEN = ''
STS_TOKEN_EXPIRES_IN = 60
AUTHENTICATION_API_VERSION = "client.authentication.k8s.io/v1alpha1"
_POD_USERNAME = 'aws'
_CONTEXT_NAME = 'aws'


class ClusterStates(Enum):
    """Contains the possible State values of an EKS Cluster."""

    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    FAILED = "FAILED"
    UPDATING = "UPDATING"
    NONEXISTENT = "NONEXISTENT"


class FargateProfileStates(Enum):
    """Contains the possible State values of an AWS Fargate profile."""

    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    CREATE_FAILED = "CREATE_FAILED"
    DELETE_FAILED = "DELETE_FAILED"
    NONEXISTENT = "NONEXISTENT"


class NodegroupStates(Enum):
    """Contains the possible State values of an EKS Managed Nodegroup."""

    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"
    CREATE_FAILED = "CREATE_FAILED"
    DELETE_FAILED = "DELETE_FAILED"
    DEGRADED = "DEGRADED"
    NONEXISTENT = "NONEXISTENT"


class EksHook(AwsBaseHook):
    """
    Interact with Amazon EKS, using the boto3 library.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    client_type = 'eks'

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = self.client_type
        super().__init__(*args, **kwargs)

    def create_cluster(
        self,
        name: str,
        roleArn: str,
        resourcesVpcConfig: Dict,
        **kwargs,
    ) -> Dict:
        """
        Creates an Amazon EKS control plane.

        :param name: The unique name to give to your Amazon EKS Cluster.
        :param roleArn: The Amazon Resource Name (ARN) of the IAM role that provides permissions
          for the Kubernetes control plane to make calls to AWS API operations on your behalf.
        :param resourcesVpcConfig: The VPC configuration used by the cluster control plane.

        :return: Returns descriptive information about the created EKS Cluster.
        :rtype: Dict
        """
        eks_client = self.conn

        response = eks_client.create_cluster(
            name=name, roleArn=roleArn, resourcesVpcConfig=resourcesVpcConfig, **kwargs
        )

        self.log.info("Created Amazon EKS cluster with the name %s.", response.get('cluster').get('name'))
        return response

    def create_nodegroup(
        self,
        clusterName: str,
        nodegroupName: str,
        subnets: List[str],
        nodeRole: Optional[str],
        *,
        tags: Optional[Dict] = None,
        **kwargs,
    ) -> Dict:
        """
        Creates an Amazon EKS managed node group for an Amazon EKS Cluster.

        :param clusterName: The name of the Amazon EKS cluster to create the EKS Managed Nodegroup in.
        :param nodegroupName: The unique name to give your managed nodegroup.
        :param subnets: The subnets to use for the Auto Scaling group that is created for your nodegroup.
        :param nodeRole: The Amazon Resource Name (ARN) of the IAM role to associate with your nodegroup.
        :param tags: Optional tags to apply to your nodegroup.

        :return: Returns descriptive information about the created EKS Managed Nodegroup.
        :rtype: Dict
        """
        eks_client = self.conn

        # The below tag is mandatory and must have a value of either 'owned' or 'shared'
        # A value of 'owned' denotes that the subnets are exclusive to the nodegroup.
        # The 'shared' value allows more than one resource to use the subnet.
        cluster_tag_key = f'kubernetes.io/cluster/{clusterName}'
        resolved_tags = tags or {}
        if cluster_tag_key not in resolved_tags:
            resolved_tags[cluster_tag_key] = 'owned'

        response = eks_client.create_nodegroup(
            clusterName=clusterName,
            nodegroupName=nodegroupName,
            subnets=subnets,
            nodeRole=nodeRole,
            tags=resolved_tags,
            **kwargs,
        )

        self.log.info(
            "Created an Amazon EKS managed node group named %s in Amazon EKS cluster %s",
            response.get('nodegroup').get('nodegroupName'),
            response.get('nodegroup').get('clusterName'),
        )
        return response

    def create_fargate_profile(
        self,
        clusterName: str,
        fargateProfileName: Optional[str],
        podExecutionRoleArn: Optional[str],
        selectors: List,
        **kwargs,
    ) -> Dict:
        """
        Creates an AWS Fargate profile for an Amazon EKS cluster.

        :param clusterName: The name of the Amazon EKS cluster to apply the Fargate profile to.
        :param fargateProfileName: The name of the Fargate profile.
        :param podExecutionRoleArn: The Amazon Resource Name (ARN) of the pod execution role to
            use for pods that match the selectors in the Fargate profile.
        :param selectors: The selectors to match for pods to use this Fargate profile.

        :return: Returns descriptive information about the created Fargate profile.
        :rtype: Dict
        """
        eks_client = self.conn

        response = eks_client.create_fargate_profile(
            clusterName=clusterName,
            fargateProfileName=fargateProfileName,
            podExecutionRoleArn=podExecutionRoleArn,
            selectors=selectors,
            **kwargs,
        )

        self.log.info(
            "Created AWS Fargate profile with the name %s for Amazon EKS cluster %s.",
            response.get('fargateProfile').get('fargateProfileName'),
            response.get('fargateProfile').get('clusterName'),
        )
        return response

    def delete_cluster(self, name: str) -> Dict:
        """
        Deletes the Amazon EKS Cluster control plane.

        :param name: The name of the cluster to delete.

        :return: Returns descriptive information about the deleted EKS Cluster.
        :rtype: Dict
        """
        eks_client = self.conn

        response = eks_client.delete_cluster(name=name)

        self.log.info("Deleted Amazon EKS cluster with the name %s.", response.get('cluster').get('name'))
        return response

    def delete_nodegroup(self, clusterName: str, nodegroupName: str) -> Dict:
        """
        Deletes an Amazon EKS managed node group from a specified cluster.

        :param clusterName: The name of the Amazon EKS Cluster that is associated with your nodegroup.
        :param nodegroupName: The name of the nodegroup to delete.

        :return: Returns descriptive information about the deleted EKS Managed Nodegroup.
        :rtype: Dict
        """
        eks_client = self.conn

        response = eks_client.delete_nodegroup(clusterName=clusterName, nodegroupName=nodegroupName)

        self.log.info(
            "Deleted Amazon EKS managed node group named %s from Amazon EKS cluster %s.",
            response.get('nodegroup').get('nodegroupName'),
            response.get('nodegroup').get('clusterName'),
        )
        return response

    def delete_fargate_profile(self, clusterName: str, fargateProfileName: str) -> Dict:
        """
        Deletes an AWS Fargate profile from a specified Amazon EKS cluster.

        :param clusterName: The name of the Amazon EKS cluster associated with the Fargate profile to delete.
        :param fargateProfileName: The name of the Fargate profile to delete.

        :return: Returns descriptive information about the deleted Fargate profile.
        :rtype: Dict
        """
        eks_client = self.conn

        response = eks_client.delete_fargate_profile(
            clusterName=clusterName, fargateProfileName=fargateProfileName
        )

        self.log.info(
            "Deleted AWS Fargate profile with the name %s from Amazon EKS cluster %s.",
            response.get('fargateProfile').get('fargateProfileName'),
            response.get('fargateProfile').get('clusterName'),
        )
        return response

    def describe_cluster(self, name: str, verbose: bool = False) -> Dict:
        """
        Returns descriptive information about an Amazon EKS Cluster.

        :param name: The name of the cluster to describe.
        :param verbose: Provides additional logging if set to True.  Defaults to False.

        :return: Returns descriptive information about a specific EKS Cluster.
        :rtype: Dict
        """
        eks_client = self.conn

        response = eks_client.describe_cluster(name=name)

        self.log.info(
            "Retrieved details for Amazon EKS cluster named %s.", response.get('cluster').get('name')
        )
        if verbose:
            cluster_data = response.get('cluster')
            self.log.info("Amazon EKS cluster details: %s", json.dumps(cluster_data, cls=AirflowJsonEncoder))
        return response

    def describe_nodegroup(self, clusterName: str, nodegroupName: str, verbose: bool = False) -> Dict:
        """
        Returns descriptive information about an Amazon EKS managed node group.

        :param clusterName: The name of the Amazon EKS Cluster associated with the nodegroup.
        :param nodegroupName: The name of the nodegroup to describe.
        :param verbose: Provides additional logging if set to True.  Defaults to False.

        :return: Returns descriptive information about a specific EKS Nodegroup.
        :rtype: Dict
        """
        eks_client = self.conn

        response = eks_client.describe_nodegroup(clusterName=clusterName, nodegroupName=nodegroupName)

        self.log.info(
            "Retrieved details for Amazon EKS managed node group named %s in Amazon EKS cluster %s.",
            response.get('nodegroup').get('nodegroupName'),
            response.get('nodegroup').get('clusterName'),
        )
        if verbose:
            nodegroup_data = response.get('nodegroup')
            self.log.info(
                "Amazon EKS managed node group details: %s",
                json.dumps(nodegroup_data, cls=AirflowJsonEncoder),
            )
        return response

    def describe_fargate_profile(
        self, clusterName: str, fargateProfileName: str, verbose: bool = False
    ) -> Dict:
        """
        Returns descriptive information about an AWS Fargate profile.

        :param clusterName: The name of the Amazon EKS Cluster associated with the Fargate profile.
        :param fargateProfileName: The name of the Fargate profile to describe.
        :param verbose: Provides additional logging if set to True.  Defaults to False.

        :return: Returns descriptive information about an AWS Fargate profile.
        :rtype: Dict
        """
        eks_client = self.conn

        response = eks_client.describe_fargate_profile(
            clusterName=clusterName, fargateProfileName=fargateProfileName
        )

        self.log.info(
            "Retrieved details for AWS Fargate profile named %s in Amazon EKS cluster %s.",
            response.get('fargateProfile').get('fargateProfileName'),
            response.get('fargateProfile').get('clusterName'),
        )
        if verbose:
            fargate_profile_data = response.get('fargateProfile')
            self.log.info(
                "AWS Fargate profile details: %s", json.dumps(fargate_profile_data, cls=AirflowJsonEncoder)
            )
        return response

    def get_cluster_state(self, clusterName: str) -> ClusterStates:
        """
        Returns the current status of a given Amazon EKS Cluster.

        :param clusterName: The name of the cluster to check.

        :return: Returns the current status of a given Amazon EKS Cluster.
        :rtype: ClusterStates
        """
        eks_client = self.conn

        try:
            return ClusterStates(eks_client.describe_cluster(name=clusterName).get('cluster').get('status'))
        except ClientError as ex:
            if ex.response.get("Error").get("Code") == "ResourceNotFoundException":
                return ClusterStates.NONEXISTENT
            raise

    def get_fargate_profile_state(self, clusterName: str, fargateProfileName: str) -> FargateProfileStates:
        """
        Returns the current status of a given AWS Fargate profile.

        :param clusterName: The name of the Amazon EKS Cluster associated with the Fargate profile.
        :param fargateProfileName: The name of the Fargate profile to check.

        :return: Returns the current status of a given AWS Fargate profile.
        :rtype: AWS FargateProfileStates
        """
        eks_client = self.conn

        try:
            return FargateProfileStates(
                eks_client.describe_fargate_profile(
                    clusterName=clusterName, fargateProfileName=fargateProfileName
                )
                .get('fargateProfile')
                .get('status')
            )
        except ClientError as ex:
            if ex.response.get("Error").get("Code") == "ResourceNotFoundException":
                return FargateProfileStates.NONEXISTENT
            raise

    def get_nodegroup_state(self, clusterName: str, nodegroupName: str) -> NodegroupStates:
        """
        Returns the current status of a given Amazon EKS managed node group.

        :param clusterName: The name of the Amazon EKS Cluster associated with the nodegroup.
        :param nodegroupName: The name of the nodegroup to check.

        :return: Returns the current status of a given Amazon EKS Nodegroup.
        :rtype: NodegroupStates
        """
        eks_client = self.conn

        try:
            return NodegroupStates(
                eks_client.describe_nodegroup(clusterName=clusterName, nodegroupName=nodegroupName)
                .get('nodegroup')
                .get('status')
            )
        except ClientError as ex:
            if ex.response.get("Error").get("Code") == "ResourceNotFoundException":
                return NodegroupStates.NONEXISTENT
            raise

    def list_clusters(
        self,
        verbose: bool = False,
    ) -> List:
        """
        Lists all Amazon EKS Clusters in your AWS account.

        :param verbose: Provides additional logging if set to True.  Defaults to False.

        :return: A List containing the cluster names.
        :rtype: List
        """
        eks_client = self.conn
        list_cluster_call = partial(eks_client.list_clusters)

        return self._list_all(api_call=list_cluster_call, response_key="clusters", verbose=verbose)

    def list_nodegroups(
        self,
        clusterName: str,
        verbose: bool = False,
    ) -> List:
        """
        Lists all Amazon EKS managed node groups associated with the specified cluster.

        :param clusterName: The name of the Amazon EKS Cluster containing nodegroups to list.
        :param verbose: Provides additional logging if set to True.  Defaults to False.

        :return: A List of nodegroup names within the given cluster.
        :rtype: List
        """
        eks_client = self.conn
        list_nodegroups_call = partial(eks_client.list_nodegroups, clusterName=clusterName)

        return self._list_all(api_call=list_nodegroups_call, response_key="nodegroups", verbose=verbose)

    def list_fargate_profiles(
        self,
        clusterName: str,
        verbose: bool = False,
    ) -> List:
        """
        Lists all AWS Fargate profiles associated with the specified cluster.

        :param clusterName: The name of the Amazon EKS Cluster containing Fargate profiles to list.
        :param verbose: Provides additional logging if set to True.  Defaults to False.

        :return: A list of Fargate profile names within a given cluster.
        :rtype: List
        """
        eks_client = self.conn
        list_fargate_profiles_call = partial(eks_client.list_fargate_profiles, clusterName=clusterName)

        return self._list_all(
            api_call=list_fargate_profiles_call, response_key="fargateProfileNames", verbose=verbose
        )

    def _list_all(self, api_call: Callable, response_key: str, verbose: bool) -> List:
        """
        Repeatedly calls a provided boto3 API Callable and collates the responses into a List.

        :param api_call: The api command to execute.
        :param response_key: Which dict key to collect into the final list.
        :param verbose: Provides additional logging if set to True.  Defaults to False.

        :return: A List of the combined results of the provided API call.
        :rtype: List
        """
        name_collection: List = []
        token = DEFAULT_PAGINATION_TOKEN

        while token is not None:
            response = api_call(nextToken=token)
            # If response list is not empty, append it to the running list.
            name_collection += filter(None, response.get(response_key))
            token = response.get("nextToken")

        self.log.info("Retrieved list of %s %s.", len(name_collection), response_key)
        if verbose:
            self.log.info("%s found: %s", response_key.title(), name_collection)

        return name_collection

    @contextmanager
    def generate_config_file(
        self,
        eks_cluster_name: str,
        pod_namespace: Optional[str],
        pod_username: Optional[str] = None,
        pod_context: Optional[str] = None,
    ) -> Generator[str, None, None]:
        """
        Writes the kubeconfig file given an EKS Cluster.

        :param eks_cluster_name: The name of the cluster to generate kubeconfig file for.
        :param pod_namespace: The namespace to run within kubernetes.
        """
        if pod_username:
            warnings.warn(
                "This pod_username parameter is deprecated, because changing the value does not make any "
                "visible changes to the user.",
                DeprecationWarning,
                stacklevel=2,
            )
        if pod_context:
            warnings.warn(
                "This pod_context parameter is deprecated, because changing the value does not make any "
                "visible changes to the user.",
                DeprecationWarning,
                stacklevel=2,
            )
        # Set up the client
        eks_client = self.conn

        # Get cluster details
        cluster = eks_client.describe_cluster(name=eks_cluster_name)
        cluster_cert = cluster["cluster"]["certificateAuthority"]["data"]
        cluster_ep = cluster["cluster"]["endpoint"]

        cluster_config = {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [
                {
                    "cluster": {"server": cluster_ep, "certificate-authority-data": cluster_cert},
                    "name": eks_cluster_name,
                }
            ],
            "contexts": [
                {
                    "context": {
                        "cluster": eks_cluster_name,
                        "namespace": pod_namespace,
                        "user": _POD_USERNAME,
                    },
                    "name": _CONTEXT_NAME,
                }
            ],
            "current-context": _CONTEXT_NAME,
            "preferences": {},
            "users": [
                {
                    "name": _POD_USERNAME,
                    "user": {
                        "exec": {
                            "apiVersion": AUTHENTICATION_API_VERSION,
                            "command": sys.executable,
                            "args": [
                                "-m",
                                "airflow.providers.amazon.aws.utils.eks_get_token",
                                *(
                                    ["--region-name", self.region_name]
                                    if self.region_name is not None
                                    else []
                                ),
                                *(
                                    ["--aws-conn-id", self.aws_conn_id]
                                    if self.aws_conn_id is not None
                                    else []
                                ),
                                "--cluster-name",
                                eks_cluster_name,
                            ],
                            "env": [
                                {
                                    "name": "AIRFLOW__LOGGING__LOGGING_LEVEL",
                                    "value": "FATAL",
                                }
                            ],
                            "interactiveMode": "Never",
                        }
                    },
                }
            ],
        }
        config_text = yaml.dump(cluster_config, default_flow_style=False)

        with tempfile.NamedTemporaryFile(mode='w') as config_file:
            config_file.write(config_text)
            config_file.flush()
            yield config_file.name

    def fetch_access_token_for_cluster(self, eks_cluster_name: str) -> str:
        session = self.get_session()
        service_id = self.conn.meta.service_model.service_id
        sts_url = (
            f'https://sts.{session.region_name}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15'
        )

        signer = RequestSigner(
            service_id=service_id,
            region_name=session.region_name,
            signing_name='sts',
            signature_version='v4',
            credentials=session.get_credentials(),
            event_emitter=session.events,
        )

        request_params = {
            'method': 'GET',
            'url': sts_url,
            'body': {},
            'headers': {'x-k8s-aws-id': eks_cluster_name},
            'context': {},
        }

        signed_url = signer.generate_presigned_url(
            request_dict=request_params,
            region_name=session.region_name,
            expires_in=STS_TOKEN_EXPIRES_IN,
            operation_name='',
        )

        base64_url = base64.urlsafe_b64encode(signed_url.encode('utf-8')).decode('utf-8')

        # remove any base64 encoding padding:
        return 'k8s-aws-v1.' + base64_url.rstrip("=")


class EKSHook(EksHook):
    """
    This hook is deprecated.
    Please use :class:`airflow.providers.amazon.aws.hooks.eks.EksHook`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This hook is deprecated. Please use `airflow.providers.amazon.aws.hooks.eks.EksHook`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
