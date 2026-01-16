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
import base64
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.signers import RequestSigner

# Presigned STS urls are valid for 15 minutes, set token expiration to 1 minute before it expires for
# some cushion
STS_TOKEN_EXPIRES_IN = 60
TOKEN_EXPIRATION_MINUTES = 14


def get_expiration_time():
    token_expiration = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_EXPIRATION_MINUTES)

    return token_expiration.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_parser():
    parser = argparse.ArgumentParser(description="Get a token for authentication with an Amazon EKS cluster.")
    parser.add_argument(
        "--cluster-name", help="The name of the cluster to generate kubeconfig file for.", required=True
    )
    parser.add_argument(
        "--region-name", help="AWS region_name. If not specified then the default boto3 behaviour is used."
    )
    parser.add_argument("--sts-url", help="Provide the STS url", required=True)

    return parser


def fetch_access_token_for_cluster(eks_cluster_name: str, sts_url: str, region_name: str) -> str:
    # This will use the credentials from the caller set as the standard AWS env variables
    session = boto3.Session(region_name=region_name)
    eks_client = session.client("eks")
    # This env variable is required so that we get a regionalized endpoint for STS in regions that
    # otherwise default to global endpoints. The mechanism below to generate the token is very picky that
    # the endpoint is regional.
    os.environ["AWS_STS_REGIONAL_ENDPOINTS"] = "regional"

    credentials = session.get_credentials()
    if credentials is None:
        raise ValueError(
            "No AWS credentials found. Credentials may have expired or not been properly configured. "
            "Please ensure AWS credentials are available through environment variables, "
            "AWS config files, or IAM roles."
        )

    signer = RequestSigner(
        service_id=eks_client.meta.service_model.service_id,
        region_name=session.region_name,
        signing_name="sts",
        signature_version="v4",
        credentials=credentials,
        event_emitter=session.events,
    )

    request_params = {
        "method": "GET",
        "url": sts_url,
        "body": {},
        "headers": {"x-k8s-aws-id": eks_cluster_name},
        "context": {},
    }

    signed_url = signer.generate_presigned_url(
        request_dict=request_params,
        region_name=session.region_name,
        expires_in=STS_TOKEN_EXPIRES_IN,
        operation_name="",
    )

    base64_url = base64.urlsafe_b64encode(signed_url.encode("utf-8")).decode("utf-8")

    # remove any base64 encoding padding:
    return "k8s-aws-v1." + base64_url.rstrip("=")


def main():
    parser = get_parser()
    args = parser.parse_args()
    access_token = fetch_access_token_for_cluster(
        args.cluster_name, args.sts_url, region_name=args.region_name
    )
    access_token_expiration = get_expiration_time()
    print(f"expirationTimestamp: {access_token_expiration}, token: {access_token}")


if __name__ == "__main__":
    main()
