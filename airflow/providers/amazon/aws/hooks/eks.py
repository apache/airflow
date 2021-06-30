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

# pylint: disable=invalid-name
"""Interact with Amazon EKS, using the boto3 library."""
import base64
import json
import re
import tempfile
from functools import partial
from typing import Callable, Dict, List, Optional

import boto3
import yaml
from botocore.exceptions import ClientError
from botocore.signers import RequestSigner

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.json import AirflowJsonEncoder

DEFAULT_CONTEXT_NAME = 'aws'
DEFAULT_PAGINATION_TOKEN = ''
DEFAULT_POD_USERNAME = 'aws'
KUBE_CONFIG_FILE_PREFIX = 'kube_config_'
STS_TOKEN_EXPIRES_IN = 60


class EKSHook(AwsBaseHook):
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

    def create_cluster(self, name: str, roleArn: str, resourcesVpcConfig: Dict, **kwargs) -> Dict:
        """
        Creates an Amazon EKS control plane.

        :param name: The unique name to give to your Amazon EKS Cluster.
        :type name: str
        :param roleArn: The Amazon Resource Name (ARN) of the IAM role that provides permissions
          for the Kubernetes control plane to make calls to AWS API operations on your behalf.
        :type roleArn: str
        :param resourcesVpcConfig: The VPC configuration used by the cluster control plane.
        :type resourcesVpcConfig: Dict

        :return: Returns descriptive information about the created EKS Cluster.
        :rtype: Dict
        """
        try:
            eks_client = self.conn

            response = eks_client.create_cluster(
                name=name, roleArn=roleArn, resourcesVpcConfig=resourcesVpcConfig, **kwargs
            )

            self.log.info("Created cluster with the name %s.", response.get('cluster').get('name'))
            return response

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e

    def create_nodegroup(
        self, clusterName: str, nodegroupName: str, subnets: List[str], nodeRole: str, **kwargs
    ) -> Dict:
        """
        Creates an Amazon EKS Managed Nodegroup for an EKS Cluster.

        :param clusterName: The name of the cluster to create the EKS Managed Nodegroup in.
        :type clusterName: str
        :param nodegroupName: The unique name to give your managed nodegroup.
        :type nodegroupName: str
        :param subnets: The subnets to use for the Auto Scaling group that is created for your nodegroup.
        :type subnets: List[str]
        :param nodeRole: The Amazon Resource Name (ARN) of the IAM role to associate with your nodegroup.
        :type nodeRole: str

        :return: Returns descriptive information about the created EKS Managed Nodegroup.
        :rtype: Dict
        """
        try:
            eks_client = self.conn
            # The below tag is mandatory and must have a value of either 'owned' or 'shared'
            # A value of 'owned' denotes that the subnets are exclusive to the nodegroup.
            # The 'shared' value allows more than one resource to use the subnet.
            tags = {'kubernetes.io/cluster/' + clusterName: 'owned'}
            if "tags" in kwargs:
                tags = {**tags, **kwargs["tags"]}
                kwargs.pop("tags")

            response = eks_client.create_nodegroup(
                clusterName=clusterName,
                nodegroupName=nodegroupName,
                subnets=subnets,
                nodeRole=nodeRole,
                tags=tags,
                **kwargs,
            )

            self.log.info(
                "Created a managed nodegroup named %s in cluster %s",
                response.get('nodegroup').get('nodegroupName'),
                response.get('nodegroup').get('clusterName'),
            )
            return response

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e

    def delete_cluster(self, name: str) -> Dict:
        """
        Deletes the Amazon EKS Cluster control plane.

        :param name: The name of the cluster to delete.
        :type name: str

        :return: Returns descriptive information about the deleted EKS Cluster.
        :rtype: Dict
        """
        try:
            eks_client = self.conn

            response = eks_client.delete_cluster(name=name)
            self.log.info("Deleted cluster with the name %s.", response.get('cluster').get('name'))
            return response

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e

    def delete_nodegroup(self, clusterName: str, nodegroupName: str) -> Dict:
        """
        Deletes an Amazon EKS Nodegroup from a specified cluster.

        :param clusterName: The name of the Amazon EKS Cluster that is associated with your nodegroup.
        :type clusterName: str
        :param nodegroupName: The name of the nodegroup to delete.
        :type nodegroupName: str

        :return: Returns descriptive information about the deleted EKS Managed Nodegroup.
        :rtype: Dict
        """
        try:
            eks_client = self.conn

            response = eks_client.delete_nodegroup(clusterName=clusterName, nodegroupName=nodegroupName)
            self.log.info(
                "Deleted nodegroup named %s from cluster %s.",
                response.get('nodegroup').get('nodegroupName'),
                response.get('nodegroup').get('clusterName'),
            )
            return response

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e

    def describe_cluster(self, name: str, verbose: Optional[bool] = False) -> Dict:
        """
        Returns descriptive information about an Amazon EKS Cluster.

        :param name: The name of the cluster to describe.
        :type name: str
        :param verbose: Provides additional logging if set to True.  Defaults to False.
        :type verbose: bool

        :return: Returns descriptive information about a specific EKS Cluster.
        :rtype: Dict
        """
        try:
            eks_client = self.conn

            response = eks_client.describe_cluster(name=name)
            self.log.info("Retrieved details for cluster named %s.", response.get('cluster').get('name'))
            if verbose:
                cluster_data = response.get('cluster')
                self.log.info("Cluster Details: %s", json.dumps(cluster_data, cls=AirflowJsonEncoder))
            return response

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e

    def describe_nodegroup(
        self, clusterName: str, nodegroupName: str, verbose: Optional[bool] = False
    ) -> Dict:
        """
        Returns descriptive information about an Amazon EKS Nodegroup.

        :param clusterName: The name of the Amazon EKS Cluster associated with the nodegroup.
        :type clusterName: str
        :param nodegroupName: The name of the nodegroup to describe.
        :type nodegroupName: str
        :param verbose: Provides additional logging if set to True.  Defaults to False.
        :type verbose: bool

        :return: Returns descriptive information about a specific EKS Nodegroup.
        :rtype: Dict
        """
        try:
            eks_client = self.conn

            response = eks_client.describe_nodegroup(clusterName=clusterName, nodegroupName=nodegroupName)
            self.log.info(
                "Retrieved details for nodegroup named %s in cluster %s.",
                response.get('nodegroup').get('nodegroupName'),
                response.get('nodegroup').get('clusterName'),
            )
            if verbose:
                nodegroup_data = response.get('nodegroup')
                self.log.info("Nodegroup Details: %s", json.dumps(nodegroup_data, cls=AirflowJsonEncoder))
            return response

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e

    def get_cluster_state(self, clusterName: str) -> str:
        """
        Returns the current status of a given Amazon EKS Cluster.

        :param clusterName: The name of the cluster to check.
        :type clusterName: str

        :return: Returns the current status of a given Amazon EKS Cluster.
        :rtype: str
        """
        eks_client = self.conn

        return eks_client.describe_cluster(name=clusterName).get('cluster').get('status')

    def get_nodegroup_state(self, clusterName: str, nodegroupName: str) -> str:
        """
        Returns the current status of a given Amazon EKS Nodegroup.

        :param clusterName: The name of the Amazon EKS Cluster associated with the nodegroup.
        :type clusterName: str
        :param nodegroupName: The name of the nodegroup to check.
        :type nodegroupName: str

        :return: Returns the current status of a given Amazon EKS Nodegroup.
        :rtype: str
        """
        eks_client = self.conn

        return (
            eks_client.describe_nodegroup(clusterName=clusterName, nodegroupName=nodegroupName)
            .get('nodegroup')
            .get('status')
        )

    def list_clusters(
        self,
        verbose: Optional[bool] = False,
    ) -> List:
        """
        Lists all Amazon EKS Clusters in your AWS account.

        :param verbose: Provides additional logging if set to True.  Defaults to False.
        :type verbose: bool

        :return: A List containing the cluster names.
        :rtype: List
        """
        eks_client = self.conn
        api_call = partial(eks_client.list_clusters)

        return self._list_all(api_call=api_call, response_key="clusters", verbose=verbose)

    def list_nodegroups(
        self,
        clusterName: str,
        verbose: Optional[bool] = False,
    ) -> List:
        """
        Lists all Amazon EKS Nodegroups associated with the specified cluster.

        :param clusterName: The name of the Amazon EKS Cluster containing nodegroups to list.
        :type clusterName: str
        :param verbose: Provides additional logging if set to True.  Defaults to False.
        :type verbose: bool

        :return: A List of nodegroup names within the given cluster.
        :rtype: List
        """
        eks_client = self.conn
        api_call = partial(eks_client.list_nodegroups, clusterName=clusterName)

        return self._list_all(api_call=api_call, response_key="nodegroups", verbose=verbose)

    def _list_all(self, api_call: Callable, response_key: str, verbose: bool) -> List:
        """
        Repeatedly calls a provided boto3 API Callable and collates the responses into a List.

        :param api_call: The api command to execute.
        :type api_call: Callable
        :param response_key: Which dict key to collect into the final list.
        :type response_key: str
        :param verbose: Provides additional logging if set to True.  Defaults to False.
        :type verbose: bool

        :return: A List of the combined results of the provided API call.
        :rtype: List
        """
        name_collection = []
        token = DEFAULT_PAGINATION_TOKEN

        try:
            while token != "null":
                response = api_call(nextToken=token)
                # If response list is not empty, append it to the running list.
                name_collection += filter(None, response.get(response_key))
                token = response.get("nextToken")

            self.log.info("Retrieved list of %s %s.", len(name_collection), response_key)
            if verbose:
                self.log.info("%s found: %s", response_key.title(), name_collection)

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e
        return name_collection


def generate_config_file(
    eks_cluster_name: str,
    eks_namespace_name: str,
    pod_name: str,
    aws_profile: Optional[str],
    pod_username: Optional[str] = DEFAULT_POD_USERNAME,
    pod_context: Optional[str] = DEFAULT_CONTEXT_NAME,
    aws_region: Optional[str] = None,
) -> str:
    """
    Writes the kubeconfig file given an EKS Cluster.

    :param eks_cluster_name: The name of the cluster to create the EKS Managed Nodegroup in.
    :type eks_cluster_name: str
    :param eks_namespace_name: The namespace to run within kubernetes.
    :type eks_namespace_name: str
    :param pod_name: The unique name to give the pod.  Used as an identifier in the config filename.
    :type pod_name: str
    :param aws_profile: The named profile containing the credentials to use.
    :type aws_profile: str
    :param pod_username: The username under which to execute the pod.
    :type pod_username: str
    :param pod_context: The name of the context access parameters to use.
    :type pod_context: str
    :param aws_region: The name of the AWS Region the EKS Cluster resides in.
    :type aws_region: str
    """
    # Set up the client
    session = boto3.Session(region_name=aws_region, profile_name=aws_profile)
    eks_client = session.client("eks")

    # Get cluster details
    cluster = eks_client.describe_cluster(name=eks_cluster_name)
    cluster_cert = cluster["cluster"]["certificateAuthority"]["data"]
    cluster_ep = cluster["cluster"]["endpoint"]

    token = _get_bearer_token(session=session, cluster_id=eks_cluster_name, aws_region=aws_region)

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
                    "namespace": eks_namespace_name,
                    "user": pod_username,
                },
                "name": pod_context,
            }
        ],
        "current-context": pod_context,
        "preferences": {},
        "users": [
            {
                "name": pod_username,
                "user": {
                    "token": token,
                },
            }
        ],
    }

    config_text = yaml.dump(cluster_config, default_flow_style=False)

    # Set the filename to something which can be found later if needed.
    filename_prefix = KUBE_CONFIG_FILE_PREFIX + pod_name
    with tempfile.NamedTemporaryFile(prefix=filename_prefix, mode='w', delete=False) as config_file:
        config_file.write(config_text)

    return config_file.name


def _get_bearer_token(session: boto3.Session, cluster_id: str, aws_region: str) -> str:
    client = session.client('sts', region_name=aws_region)
    service_id = client.meta.service_model.service_id

    signer = RequestSigner(
        service_id=service_id,
        region_name=aws_region,
        signing_name='sts',
        signature_version='v4',
        credentials=session.get_credentials(),
        event_emitter=session.events,
    )

    request_params = {
        'method': 'GET',
        'url': f'https://sts.{aws_region}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15',
        'body': {},
        'headers': {'x-k8s-aws-id': cluster_id},
        'context': {},
    }

    signed_url = signer.generate_presigned_url(
        request_dict=request_params,
        region_name=aws_region,
        expires_in=STS_TOKEN_EXPIRES_IN,
        operation_name='',
    )

    base64_url = base64.urlsafe_b64encode(signed_url.encode('utf-8')).decode('utf-8')

    # remove any base64 encoding padding:
    return 'k8s-aws-v1.' + re.sub(r'=*', '', base64_url)
