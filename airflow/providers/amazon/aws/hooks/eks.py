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

import json
from typing import Dict, List, Optional

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.json import AirflowJsonEncoder

DEFAULT_RESULTS_PER_PAGE = 100
DEFAULT_PAGINATION_TOKEN = ''


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
        maxResults: Optional[int] = DEFAULT_RESULTS_PER_PAGE,
        nextToken: Optional[str] = DEFAULT_PAGINATION_TOKEN,
        verbose: Optional[bool] = False,
    ) -> Dict:
        """
        Lists the Amazon EKS Clusters in your AWS account.

        :param maxResults: The maximum number of results returned by ListClusters in paginated output.
        :type maxResults: int
        :param nextToken: The nextToken value returned from a previous paginated ListClusters request.
        :type nextToken: str
        :param verbose: Provides additional logging if set to True.  Defaults to False.
        :type verbose: bool

        :return: A Dictionary containing a list of cluster names and a string containing
           the next token if paginated.
        :rtype: Dict
        """
        try:
            eks_client = self.conn

            response = eks_client.list_clusters(maxResults=maxResults, nextToken=nextToken)
            cluster_list = response.get('clusters')

            self.log.info("Retrieved list of %s clusters.", len(cluster_list))
            if verbose:
                self.log.info("Clusters found: %s", cluster_list)
            return response

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e

    def list_nodegroups(
        self,
        clusterName: str,
        maxResults: Optional[int] = DEFAULT_RESULTS_PER_PAGE,
        nextToken: Optional[str] = DEFAULT_PAGINATION_TOKEN,
        verbose: Optional[bool] = False,
    ) -> Dict:
        """
        Lists the Amazon EKS Nodegroups associated with the specified
        cluster in your AWS account in the specified Region.

        :param clusterName: The name of the Amazon EKS Cluster containing nodegroups to list.
        :type clusterName: str
        :param maxResults: The maximum number of results returned by ListNodegroups in paginated output.
        :type maxResults: int
        :param nextToken: The nextToken value returned from a previous paginated ListNodegroups request.
        :type nextToken: str
        :param verbose: Provides additional logging if set to True.  Defaults to False.
        :type verbose: bool

        :return: A Dictionary containing a list of nodegroup names and a string containing
           the next token if paginated.
        :rtype: Dict
        """
        try:
            eks_client = self.conn

            response = eks_client.list_nodegroups(
                clusterName=clusterName, maxResults=maxResults, nextToken=nextToken
            )
            nodegroup_list = response.get('nodegroups')

            self.log.info("Retrieved list of %s nodegroups.", len(nodegroup_list))
            if verbose:
                self.log.info("Nodegroups found: %s", nodegroup_list)
            return response

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            raise e
