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
import base64
import os
import re
from shutil import which
from typing import Optional

import boto3
import yaml
from botocore.signers import RequestSigner

HOME = os.environ.get('HOME', '/tmp')
DEFAULT_KUBE_CONFIG_FILENAME = 'config'
DEFAULT_KUBE_CONFIG_PATH = str(os.path.join(HOME, '/.kube/', DEFAULT_KUBE_CONFIG_FILENAME))
DEFAULT_CONTEXT_NAME = 'aws'
DEFAULT_NAMESPACE_NAME = 'default'
DEFAULT_POD_USERNAME = 'aws'


STS_TOKEN_EXPIRES_IN = 60


def get_bearer_token(session, cluster_id, region):
    client = session.client('sts', region_name=region)
    service_id = client.meta.service_model.service_id

    signer = RequestSigner(service_id, region, 'sts', 'v4', session.get_credentials(), session.events)

    params = {
        'method': 'GET',
        'url': f'https://sts.{region}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15',
        'body': {},
        'headers': {'x-k8s-aws-id': cluster_id},
        'context': {},
    }

    signed_url = signer.generate_presigned_url(
        params, region_name=region, expires_in=STS_TOKEN_EXPIRES_IN, operation_name=''
    )

    base64_url = base64.urlsafe_b64encode(signed_url.encode('utf-8')).decode('utf-8')

    # remove any base64 encoding padding:
    return 'k8s-aws-v1.' + re.sub(r'=*', '', base64_url)


def generate_config_file(
    eks_cluster_name: str,
    eks_namespace_name: str,
    aws_profile: Optional[str],
    kube_config_file_location: Optional[str] = DEFAULT_KUBE_CONFIG_PATH,
    pod_username: Optional[str] = DEFAULT_POD_USERNAME,
    pod_context: Optional[str] = DEFAULT_CONTEXT_NAME,
    aws_region: Optional[str] = None,
) -> None:
    """
    Writes the kubeconfig file given an EKS Cluster name, AWS region, and file path.

    :param eks_cluster_name: The name of the cluster to create the EKS Managed Nodegroup in.
    :type eks_cluster_name: str
    :param eks_namespace_name: The namespace to run within kubernetes.
    :type eks_namespace_name: str
    :param aws_profile: The named profile containing the credentials for the AWS CLI tool to use.
    :type aws_profile: str
    :param kube_config_file_location: Path to save the generated kube_config file to.
    :type kube_config_file_location: str
    :param pod_username: The username under which to execute the pod.
    :type pod_username: str
    :param pod_context: The name of the context access parameters to use.
    :type pod_context: str
    :param role_arn: The Amazon Resource Name (ARN) of the IAM role to associate with your nodegroup.
    :type role_arn: str
    :param aws_region: The name of the AWS Region the EKS Cluster resides in.
    :type aws_region: str
    """
    installed = which("aws")
    if installed is None:
        message = (
            "AWS CLI version 2 must be installed on the worker.  See: "
            "https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html"
        )
        raise UnmetDependency(message)

    # Set up the client
    session = boto3.Session(region_name=aws_region, profile_name=aws_profile)
    eks_client = session.client("eks")

    # get cluster details
    cluster = eks_client.describe_cluster(name=eks_cluster_name)
    cluster_cert = cluster["cluster"]["certificateAuthority"]["data"]
    cluster_ep = cluster["cluster"]["endpoint"]

    token = get_bearer_token(session, eks_cluster_name, aws_region)

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
    with open(kube_config_file_location, "w") as config_file:
        config_file.write(config_text)


class UnmetDependency(Exception):
    """Thrown if the required AWS CLI tool is not installed."""
