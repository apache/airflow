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
import logging
from functools import partial
from typing import Dict, Callable, Any

import requests
from requests import HTTPError

from airflow.io.exceptions import SignError
from botocore import UNSIGNED
from botocore.awsrequest import AWSRequest

from fsspec import AbstractFileSystem

from airflow.io import Properties, TOKEN
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

S3_PROXY_URI = "s3.proxy-uri"

log = logging.getLogger(__name__)

schemes = ["s3", "s3a", "s3n"]


def get_fs(conn_id: str | None) -> AbstractFileSystem:
    aws = AwsGenericHook(aws_conn_id=conn_id)
    from s3fs import S3FileSystem

    client_kwargs = {
        "endpoint_url": aws.conn_config.endpoint_url,
        "aws_access_key_id": aws.conn_config.aws_access_key_id,
        "aws_secret_access_key": aws.conn_config.aws_secret_access_key,
        "aws_session_token": aws.conn_config.aws_session_token,
        "region_name": aws.conn_config.region_name,
    }
    config_kwargs = {}
    register_events: Dict[str, Callable[[Properties], None]] = {}

    if signer := aws.conn_config.extra_config.get("s3.signer"):
        log.info("Loading signer %s", signer)
        if singer_func := SIGNERS.get(signer):
            properties: Properties = {
                "uri": aws.conn_config.extra_config.get("s3.signer_uri"),
                TOKEN: aws.conn_config.extra_config.get("s3.signer_token"),
            }
            singer_func_with_properties = partial(singer_func, properties)
            register_events["before-sign.s3"] = singer_func_with_properties

            # Disable the AWS Signer
            config_kwargs["signature_version"] = UNSIGNED
        else:
            raise ValueError(f"Signer not available: {signer}")

    if proxy_uri := aws.conn_config.extra_config.get(S3_PROXY_URI):
        config_kwargs["proxies"] = {"http": proxy_uri, "https": proxy_uri}

    fs = S3FileSystem(client_kwargs=client_kwargs, config_kwargs=config_kwargs)

    for event_name, event_function in register_events.items():
        fs.s3.meta.events.register_last(event_name, event_function, unique_id=1925)

    return fs


def s3v4_rest_signer(properties: Properties, request: AWSRequest, **_: Any) -> AWSRequest:
    if TOKEN not in properties:
        raise SignError("Signer set, but token is not available")

    signer_url = properties["uri"].rstrip("/")
    signer_headers = {"Authorization": f"Bearer {properties[TOKEN]}"}
    signer_body = {
        "method": request.method,
        "region": request.context["client_region"],
        "uri": request.url,
        "headers": {key: [val] for key, val in request.headers.items()},
    }

    response = requests.post(f"{signer_url}/v1/aws/s3/sign", headers=signer_headers, json=signer_body)
    try:
        response.raise_for_status()
        response_json = response.json()
    except HTTPError as e:
        raise SignError(f"Failed to sign request {response.status_code}: {signer_body}") from e

    for key, value in response_json["headers"].items():
        request.headers.add_header(key, ", ".join(value))

    request.url = response_json["uri"]

    return request


SIGNERS: Dict[str, Callable[[Properties, AWSRequest], AWSRequest]] = {"S3V4RestSigner": s3v4_rest_signer}
