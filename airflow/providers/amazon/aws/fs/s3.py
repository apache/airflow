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

import asyncio
import logging
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict

import requests
from botocore import UNSIGNED
from requests import HTTPError

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

if TYPE_CHECKING:
    from botocore.awsrequest import AWSRequest
    from fsspec import AbstractFileSystem


Properties = Dict[str, str]

S3_PROXY_URI = "proxy-uri"

log = logging.getLogger(__name__)

schemes = ["s3", "s3a", "s3n"]


class SignError(Exception):
    """Raises when unable to sign a S3 request."""


def get_fs(conn_id: str | None, storage_options: dict[str, str] | None = None) -> AbstractFileSystem:
    try:
        from s3fs import S3FileSystem
    except ImportError:
        raise ImportError(
            "Airflow FS S3 protocol requires the s3fs library, but it is not installed as it requires"
            "aiobotocore. Please install the s3 protocol support library by running: "
            "pip install apache-airflow-providers-amazon[s3fs]"
        )

    s3_hook = S3Hook(aws_conn_id=conn_id)
    session = s3_hook.get_session(deferrable=True)
    endpoint_url = s3_hook.conn_config.get_service_endpoint_url(service_name="s3")

    config_kwargs: dict[str, Any] = s3_hook.conn_config.extra_config.get("config_kwargs", {})
    config_kwargs.update(storage_options or {})

    register_events: dict[str, Callable[[Properties], None]] = {}

    s3_service_config = s3_hook.service_config
    if signer := s3_service_config.get("signer", None):
        log.info("Loading signer %s", signer)
        if singer_func := SIGNERS.get(signer):
            uri = s3_service_config.get("signer_uri", None)
            token = s3_service_config.get("signer_token", None)
            if not uri or not token:
                raise ValueError(f"Signer {signer} requires uri and token")

            properties: Properties = {
                "uri": uri,
                "token": uri,
            }
            singer_func_with_properties = partial(singer_func, properties)
            register_events["before-sign.s3"] = singer_func_with_properties

            # Disable the AWS Signer
            config_kwargs["signature_version"] = UNSIGNED
        else:
            raise ValueError(f"Signer not available: {signer}")

    if proxy_uri := s3_service_config.get(S3_PROXY_URI, None):
        config_kwargs["proxies"] = {"http": proxy_uri, "https": proxy_uri}

    anon = False
    if asyncio.run(session.get_credentials()) is None:
        log.info("No credentials found, using anonymous access")
        anon = True

    fs = S3FileSystem(session=session, config_kwargs=config_kwargs, endpoint_url=endpoint_url, anon=anon)

    for event_name, event_function in register_events.items():
        fs.s3.meta.events.register_last(event_name, event_function, unique_id=1925)

    return fs


def s3v4_rest_signer(properties: Properties, request: AWSRequest, **_: Any) -> AWSRequest:
    if "token" not in properties:
        raise SignError("Signer set, but token is not available")

    signer_url = properties["uri"].rstrip("/")
    signer_headers = {"Authorization": f"Bearer {properties['token']}"}
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


SIGNERS: dict[str, Callable[[Properties, AWSRequest], AWSRequest]] = {"S3V4RestSigner": s3v4_rest_signer}
