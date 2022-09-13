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

import argparse
import json
from datetime import datetime, timedelta, timezone

from airflow.providers.amazon.aws.hooks.eks import EksHook

# Presigned STS urls are valid for 15 minutes, set token expiration to 1 minute before it expires for
# some cushion
TOKEN_EXPIRATION_MINUTES = 14


def get_expiration_time():
    token_expiration = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_EXPIRATION_MINUTES)

    return token_expiration.strftime('%Y-%m-%dT%H:%M:%SZ')


def get_parser():
    parser = argparse.ArgumentParser(description='Get a token for authentication with an Amazon EKS cluster.')
    parser.add_argument(
        '--cluster-name', help='The name of the cluster to generate kubeconfig file for.', required=True
    )
    parser.add_argument(
        '--aws-conn-id',
        help=(
            'The Airflow connection used for AWS credentials. '
            'If not specified or empty then the default boto3 behaviour is used.'
        ),
    )
    parser.add_argument(
        '--region-name', help='AWS region_name. If not specified then the default boto3 behaviour is used.'
    )

    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    eks_hook = EksHook(aws_conn_id=args.aws_conn_id, region_name=args.region_name)
    access_token = eks_hook.fetch_access_token_for_cluster(args.cluster_name)
    access_token_expiration = get_expiration_time()
    exec_credential_object = {
        "kind": "ExecCredential",
        "apiVersion": "client.authentication.k8s.io/v1alpha1",
        "spec": {},
        "status": {"expirationTimestamp": access_token_expiration, "token": access_token},
    }
    print(json.dumps(exec_credential_object))


if __name__ == '__main__':
    main()
