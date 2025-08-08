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
from datetime import datetime, timedelta
from typing import Any, Literal, TypedDict
from urllib.parse import urlparse, urlunparse

import structlog
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from airflow._shared.timezones.timezone import utcnow
from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLDetailResponse,
    UpdateHITLDetailPayload,
)
from airflow.api_fastapi.core_api.services.hitl import _get_task_instance
from airflow.configuration import conf

log = structlog.get_logger(__name__)

HITL_LINK_TYPE = Literal["ui_redirect", "perform_action"]


class HITLSharedDataOrigin(TypedDict):
    """The type of data used to generate a share token."""

    ti_id: str
    dag_id: str
    dag_run_id: str
    task_id: str
    map_index: int | None
    link_type: HITL_LINK_TYPE
    chosen_options: list[str]
    params_input: dict[str, Any]
    expires_at: str


def check_hitl_shared_link_enabled() -> None:
    """
    Check if 'api.hitl_enable_shared_links' is set to True in the config.

    This normally won't happen as the route itself won't be registered at the first place.
    """
    if not conf.getboolean("api", "hitl_enable_shared_links", fallback=False):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )


def encode_shared_data(
    # task instance
    ti_id: str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int | None,
    # Human-in-the-look shared link
    link_type: HITL_LINK_TYPE,
    expires_at: str,
    chosen_options: list[str],
    params_input: dict[str, Any],
) -> str:
    """
    Generate a secure token for Human-in-the-loop shared links.

    :param ti_id: task_instance ID
    :param dag_id: Dag ID
    :param dag_run_id: Dag run ID
    :param task_id: Task ID
    :param map_index: Map index for mapped tasks
    :param link_type: Type of link ('ui_redirect' or 'perform_action')
    :param chosen_options: Chosen options for perform_action links
    :param params_input: Parameters input for perform_action links
    :param expires_at: Custom expiration time in hours
    :param session: Database session (required to get task instance UUID)

    :return: jwt-encoded token
    """
    # TODO: make it a decorator
    check_hitl_shared_link_enabled()

    token_data: HITLSharedDataOrigin = {
        # task instance
        "ti_id": ti_id,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "map_index": map_index,
        # hitl shared link
        "link_type": link_type,
        "chosen_options": chosen_options or [],
        "params_input": params_input or {},
        "expires_at": expires_at.isoformat(),
    }

    token_json = json.dumps(token_data, sort_keys=True)
    token_bytes = token_json.encode("utf-8")
    # TODO: replace base64 as jwt
    token = base64.urlsafe_b64encode(token_bytes).decode("utf-8")
    return token


def decode_shared_data(token: str) -> HITLSharedDataOrigin:
    """
    Decode a shared link token.

    :param token: Base64-encoded token
    :return: Decoded token data
    :raises ValueError: If token is invalid or expired
    """
    check_hitl_shared_link_enabled()

    # TODO: replace base64 as jwt
    token_bytes = base64.urlsafe_b64decode(token)
    token_json = token_bytes.decode("utf-8")
    return json.loads(token_json)


def _generate_hitl_shared_link(
    # task instance
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int | None = None,
    # Human-in-the-look shared link
    base_url: str | None = None,
    link_type: HITL_LINK_TYPE = "perform_action",
    expires_at: datetime | None = None,
    chosen_options: list[str] | None = None,
    params_input: dict[str, Any] | None = None,
    # utility
    session: Session | None = None,
) -> str:
    """
    Generate a shared link for a Human-in-the-loop task instance.

    :param dag_id: Dag ID
    :param dag_run_id: Dag run ID
    :param task_id: Task ID
    :param link_type: Type of link ('ui_redirect' or 'direct_action')
    :param action: Action to perform (for direct_action links)
    :param chosen_options: Chosen options for direct_action links
    :param params_input: Parameters input for direct_action links
    :param map_index: Map index for mapped tasks
    :param expiration_hours: Custom expiration time in hours
    :param base_url: Base URL for the link
    :param session: Database session
    :return: Link data including URL and metadata
    """
    check_hitl_shared_link_enabled()

    if session is None:
        raise ValueError("Database session is required to generate shared link")

    if base_url is None and not (base_url := conf.get("api", "base_url")):
        raise ValueError("API base_url is not configured")

    if expires_at is None:
        hitl_shared_link_expiration_hours = conf.getint(
            "api", "hitl_shared_link_expiration_hours", fallback=24
        )
        expires_at = utcnow() + timedelta(hours=hitl_shared_link_expiration_hours)

    # Get the task instance to use its UUID
    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        session=session,
    )

    token = encode_shared_data(
        # task instance
        ti_id=str(task_instance.id),
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        # hitl shared link
        link_type=link_type,
        chosen_options=chosen_options or [],
        params_input=params_input or {},
        expires_at=expires_at.isoformat(),
    )

    if link_type == "perform_action":
        url_path = f"/api/v2/hitlSharedLinks/execute/{token}"
    elif link_type == "ui_redirect":
        url_path = f"/api/v2/hitlSharedLinks/redirect/{token}"
    else:
        raise ValueError(f"Unknown link type: {link_type}")

    # TODO: add back token validate_shared_link_token

    parsed_base_url = urlparse(base_url)
    return urlunparse(
        (
            parsed_base_url.scheme,
            parsed_base_url.netloc,
            url_path,
            "",
            "",
            "",
        ),
    )


def service_execute_shared_link_action(
    token: str,
    session: Session,
) -> HITLDetailResponse:
    """
    Execute an action via shared link by making PATCH request to existing HITL endpoint.

    :param token: Shared link token
    :param session: Database session
    :return: HITL detail response
    """
    check_hitl_shared_link_enabled()

    try:
        # Validate token
        token_data = validate_shared_link_token(token)

        if token_data["type"] != "direct_action":
            raise ValueError("This link is not a direct_action link")

        if not token_data.get("action"):
            raise ValueError("Action is required for direct_action links")

        # Extract action data
        chosen_options = token_data.get("chosen_options") or []
        params_input = token_data.get("params_input") or {}

        # Create update payload
        update_payload = UpdateHITLDetailPayload(
            chosen_options=chosen_options,
            params_input=params_input,
            try_number=token_data["try_number"],
        )

        # Execute the action using existing HITL service (PATCH to existing endpoint)
        return service_update_hitl_detail(
            dag_id=token_data["dag_id"],
            dag_run_id=token_data["dag_run_id"],
            task_id=token_data["task_id"],
            try_number=token_data["try_number"],
            session=session,
            update_hitl_detail_payload=update_payload,
            user=None,  # No user for shared links
            map_index=token_data.get("map_index"),
        )

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Invalid token: {e}",
        )


def service_redirect_shared_link(
    token: str,
    base_url: str | None = None,
) -> str:
    """
    Generate redirect URL to Airflow UI for HITL task interaction.

    :param token: Shared link token
    :param base_url: Base URL for Airflow instance
    :return: Redirect URL to Airflow UI
    """
    check_hitl_shared_link_enabled()

    try:
        # Validate token
        token_data = validate_shared_link_token(token)

        if token_data["type"] != "ui_redirect":
            raise ValueError("This link is not a ui_redirect link")

        if base_url is None:
            base_url = conf.get("api", "base_url")
            if not base_url:
                raise ValueError("API base_url is not configured")

        # Build redirect URL to Airflow UI
        dag_id = token_data["dag_id"]
        dag_run_id = token_data["dag_run_id"]
        task_id = token_data["task_id"]
        map_index = token_data.get("map_index")

        # Redirect to Airflow grid view with task focus
        if map_index is not None:
            redirect_url = f"{base_url.rstrip('/')}/dags/{dag_id}/grid?task_id={task_id}&dag_run_id={dag_run_id}&map_index={map_index}"
        else:
            redirect_url = (
                f"{base_url.rstrip('/')}/dags/{dag_id}/grid?task_id={task_id}&dag_run_id={dag_run_id}"
            )

        return redirect_url

    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )
