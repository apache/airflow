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
"""Services for HITL shared links functionality."""

from __future__ import annotations

import base64
import json
import os
from collections.abc import Callable
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Literal, TypedDict
from urllib.parse import urlparse, urlunparse

import structlog
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import HTTPException, Request, status
from sqlalchemy.orm import Session

from airflow._shared.timezones.timezone import utcnow
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLDetailResponse,
    UpdateHITLDetailPayload,
)
from airflow.api_fastapi.core_api.datamodels.hitl_shared_links import (
    GenerateHITLSharedLinkRequest,
    HITLSharedLinkResponse,
)
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.api_fastapi.core_api.services.public.hitl import (
    _get_task_instance,
    update_hitl_detail_through_payload,
)
from airflow.configuration import conf
from airflow.exceptions import HITLSharedLinkTimeout

log = structlog.get_logger(__name__)

HITL_LINK_TYPE = Literal["redirect", "respond"]


class HITLSharedLinkConfig(TypedDict):
    """Configuration for generating a Human-in-the-look shared link."""

    link_type: HITL_LINK_TYPE
    chosen_options: list[str]
    params_input: dict[str, Any]
    expires_at: str


class HITLSharedLinkData(TypedDict):
    """The data used to generate a Human-in-the-look shared token."""

    # task instance identifier
    ti_id: str
    dag_id: str
    dag_run_id: str
    task_id: str
    map_index: int | None

    shared_link_config: HITLSharedLinkConfig


def hitl_shared_link_enabled(func: Callable) -> Callable:
    """Check if 'api.hitl_enable_shared_links' is set to True in the config."""

    @wraps(func)
    def wrapper(*args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Any:
        if not conf.getboolean("api", "hitl_enable_shared_links", fallback=False):
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                (
                    "Human-in-the-look link sharing is disabled. "
                    "To enable, please set 'api.hitl_enable_shared_links' to true"
                ),
            )
        return func(*args, **kwargs)

    return wrapper


def encode_shared_data(
    *,
    # encrypt key
    secret_key: str,
    # task instance identifier
    ti_id: str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int | None,
    # link generatinon config
    shared_link_config: HITLSharedLinkConfig,
) -> str:
    """
    Generate a secure token for Human-in-the-loop shared links.

    :param ti_id: task_instance ID
    :param dag_id: Dag ID
    :param dag_run_id: Dag run ID
    :param task_id: Task ID
    :param map_index: Map index for mapped tasks
    :param shared_link_config: the configuration for generating a Human-in-the-loop shared token.
        link_type: Type of link ('redirect' or 'respond')
        chosen_options: Chosen options for respond links
        params_input: Parameters input for respond links
        expires_at: Custom expiration time in hours
    :param session: Database session (required to get task instance UUID)

    :return: jwt-encoded token
    """
    token_data: HITLSharedLinkData = HITLSharedLinkData(
        ti_id=ti_id,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        shared_link_config=shared_link_config,
    )

    encoded_json = json.dumps(token_data).encode()
    aesgcm = AESGCM(secret_key.encode())
    iv = os.urandom(12)
    ciphertext = aesgcm.encrypt(iv, encoded_json, None)
    return base64.urlsafe_b64encode(iv + ciphertext).decode("utf-8")


def decode_shared_data(
    *,
    secret_key: str,
    token: str,
) -> HITLSharedLinkData:
    """
    Decode a shared link token.

    :param token: Base64-encoded token
    :return: Decoded token data
    :raises ValueError: If token is invalid or expired
    """
    raw = base64.urlsafe_b64decode(token.encode())
    if len(raw) < 13:
        raise ValueError("Token too short")

    iv, ciphertext = raw[:12], raw[12:]
    aesgcm = AESGCM(secret_key.encode())
    plaintext = aesgcm.decrypt(iv, ciphertext, None)
    return json.loads(plaintext)


def infer_base_url(base_url: str) -> str:
    if not base_url:
        base_url = conf.get("api", "base_url")
        if not base_url:
            raise ValueError("API base_url is not configured")
    return base_url


def infer_expires_at(expires_at: datetime | None) -> datetime:
    if expires_at is None:
        hitl_shared_link_expiration_hours = conf.getint(
            "api", "hitl_shared_link_expiration_hours", fallback=24
        )
        expires_at = utcnow() + timedelta(hours=hitl_shared_link_expiration_hours)
    return expires_at


def validate_redirect_config(
    *, chosen_options: list[str] | None, params_input: dict[str, Any] | None
) -> None:
    if chosen_options or params_input:
        raise ValueError(
            '"chosen_options" and "params_input" should be empty "when "link_type" is set to "redirect"'
        )


def validate_respond_config(*, chosen_options: list[str] | None) -> None:
    if not chosen_options:
        raise ValueError(
            '"chosen_options" and "params_input" should be empty "when "link_type" is set to "redirect"'
        )


def validate_hitl_shared_link_config(
    *,
    link_type: HITL_LINK_TYPE,
    chosen_options: list[str] | None,
    params_input: dict[str, Any] | None,
    expires_at: str,
) -> None:
    if (expires_at_dt := datetime.fromisoformat(expires_at)) and expires_at_dt < utcnow():
        raise HITLSharedLinkTimeout()

    if link_type == "redirect":
        validate_redirect_config(chosen_options=chosen_options, params_input=params_input)
    elif link_type == "respond":
        validate_respond_config(chosen_options=chosen_options)
    else:
        raise ValueError(f"Invalid link_type {link_type}")


def validate_hitl_shared_link_data(shared_data: HITLSharedLinkData, session: Session) -> None:
    shared_link_config: HITLSharedLinkConfig = shared_data["shared_link_config"]
    validate_hitl_shared_link_config(
        link_type=shared_link_config["link_type"],
        chosen_options=shared_link_config["chosen_options"],
        params_input=shared_link_config["params_input"],
        expires_at=shared_link_config["expires_at"],
    )

    task_instance = _get_task_instance(
        dag_id=shared_data["dag_id"],
        dag_run_id=shared_data["dag_run_id"],
        task_id=shared_data["task_id"],
        map_index=shared_data["map_index"],
        session=session,
    )

    if task_instance.id != shared_data["ti_id"]:
        raise ValueError("task instance id does not match")


def _generate_hitl_shared_link(
    secret_key: str,
    # task instance required
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    # task instance required
    link_type: HITL_LINK_TYPE,
    base_url: str,
    # utility
    session: Session,
    # task instance optional
    map_index: int | None = None,
    # Human-in-the-look shared link
    expires_at: datetime | None = None,
    chosen_options: list[str] | None = None,
    params_input: dict[str, Any] | None = None,
) -> str:
    """
    Generate a shared link for a Human-in-the-loop task instance.

    :param dag_id: Dag ID
    :param dag_run_id: Dag run ID
    :param task_id: Task ID
    :param map_index: Map index for mapped tasks
    :param base_url: Base URL for the link
    :param link_type: Type of link ('redirect' or 'respond')
    :param expires_at: Custom expiration time. (default to 1 day after)
    :param chosen_options: Chosen options for 'respond' links
    :param params_input: Parameters input for 'respond' links
    :param session: Database session

    :return: Link data including URL and metadata
    """
    base_url = infer_base_url(base_url)
    expires_at = infer_expires_at(expires_at)

    validate_hitl_shared_link_config(
        link_type=link_type,
        chosen_options=chosen_options,
        params_input=params_input,
        expires_at=expires_at.isoformat(),
    )

    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        session=session,
    )

    token = encode_shared_data(
        secret_key=secret_key,
        # task instance
        ti_id=str(task_instance.id),
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        shared_link_config=HITLSharedLinkConfig(
            link_type=link_type,
            chosen_options=chosen_options or [],
            params_input=params_input or {},
            expires_at=expires_at.isoformat(),
        ),
    )

    hitl_shared_links_endpoint_base = "/api/v2/hitlSharedLinks"
    parsed_base_url = urlparse(base_url)
    return urlunparse(
        (
            parsed_base_url.scheme,
            parsed_base_url.netloc,
            f"{hitl_shared_links_endpoint_base}/{link_type}/{token}",
            "",
            "",
            "",
        ),
    )


def retrieve_redirect_url_from_token(
    *,
    token: str,
    secret_key: str,
    base_url: str,
    session: Session,
) -> str:
    """
    Retrieve the redirect URL to Airflow UI through token.

    :param token: Shared link token
    :param base_url: Base URL for Airflow instance

    :return: Redirect URL to Airflow UI
    """
    shared_data = decode_shared_data(token=token, secret_key=secret_key)
    shared_link_config = shared_data["shared_link_config"]
    if (link_type := shared_link_config["link_type"]) != "redirect":
        raise ValueError(f"Unexpected link_type '{link_type}'")

    validate_hitl_shared_link_data(shared_data=shared_data, session=session)

    base_url = infer_base_url(base_url)
    dag_id, dag_run_id, task_id, map_index = (
        shared_data["dag_id"],
        shared_data["dag_run_id"],
        shared_data["task_id"],
        shared_data["map_index"],
    )

    url_ti_part = f"/dags/{dag_id}/runs/{dag_run_id}/tasks/{task_id}"
    if map_index is not None:
        url_ti_part = f"{url_ti_part}/{map_index}"

    parsed_base_url = urlparse(base_url)
    return urlunparse(
        (
            parsed_base_url.scheme,
            parsed_base_url.netloc,
            f"{url_ti_part}/required_actions",
            "",
            "",
            "",
        ),
    )


def update_hitl_detail_through_token(
    *,
    token: str,
    secret_key: str,
    user: GetUserDep,
    session: SessionDep,
) -> HITLDetailResponse:
    shared_data = decode_shared_data(token=token, secret_key=secret_key)
    shared_link_config = shared_data["shared_link_config"]
    if (link_type := shared_link_config["link_type"]) != "respond":
        raise ValueError(f"Unexpected link_type '{link_type}'")

    validate_hitl_shared_link_data(shared_data=shared_data, session=session)
    return update_hitl_detail_through_payload(
        dag_id=shared_data["dag_id"],
        dag_run_id=shared_data["dag_run_id"],
        task_id=shared_data["task_id"],
        map_index=shared_data["map_index"],
        update_hitl_detail_payload=UpdateHITLDetailPayload(
            chosen_options=shared_link_config["chosen_options"],
            params_input=shared_link_config["params_input"],
        ),
        session=session,
        user=user,
    )


def generate_hitl_shared_link(
    # task instance identifier
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int | None,
    # payload
    generate_shared_link_request: GenerateHITLSharedLinkRequest,
    # utitliy
    request: Request,
    session: SessionDep,
) -> HITLSharedLinkResponse:
    expires_at = infer_expires_at(generate_shared_link_request.expires_at)

    try:
        link_url = _generate_hitl_shared_link(
            # task instance identifier
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            map_index=map_index,
            # Human-in-the-look shared link config
            base_url=str(request.base_url),
            link_type=generate_shared_link_request.link_type,
            expires_at=expires_at,
            chosen_options=generate_shared_link_request.chosen_options,
            params_input=generate_shared_link_request.params_input,
            # utility
            secret_key=request.app.state.secret_key,
            session=session,
        )
    except ValueError as err:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(err),
        )
    except HITLSharedLinkTimeout:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Set 'expires_at' at a past time is not allowed.",
        )
    return HITLSharedLinkResponse(
        url=HttpUrl(link_url),
        expires_at=expires_at,
    )
