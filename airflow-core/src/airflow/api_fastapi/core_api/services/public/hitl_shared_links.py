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
from typing import Any

import structlog
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLDetailResponse,
    UpdateHITLDetailPayload,
)
from airflow.api_fastapi.core_api.services.public.hitl import (
    service_update_hitl_detail,
)
from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone

log = structlog.get_logger(__name__)


def requires_hitl_shared_links_enabled() -> None:
    """Check if HITL shared links are enabled."""
    if not conf.getboolean("api", "hitl_enable_shared_links", fallback=False):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )


def _get_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Session,
    map_index: int | None = None,
    try_number: int = 1,
) -> TaskInstance:
    """
    Get a task instance by its identifiers.

    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID
    :param session: Database session
    :param map_index: Map index for mapped tasks
    :param try_number: Try number for the task
    :return: Task instance
    """
    query = select(TaskInstance).where(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == dag_run_id,
        TaskInstance.task_id == task_id,
        TaskInstance.try_number == try_number,
    )

    if map_index is not None:
        query = query.where(TaskInstance.map_index == map_index)

    task_instance = session.scalar(query)

    if not task_instance:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Task instance not found: {dag_id}/{dag_run_id}/{task_id}"
            + (f"/{map_index}" if map_index is not None else "")
            + f" (try_number={try_number})",
        )

    return task_instance


def generate_shared_link_token(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int,
    link_type: str = "direct_action",
    action: str | None = None,
    chosen_options: list[str] | None = None,
    params_input: dict[str, Any] | None = None,
    map_index: int | None = None,
    expiration_hours: int | None = None,
    session: Session | None = None,
) -> str:
    """
    Generate a secure token for HITL shared links.

    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID
    :param try_number: Try number for the task
    :param link_type: Type of link ('ui_redirect' or 'direct_action')
    :param action: Action to perform (for direct_action links)
    :param chosen_options: Chosen options for direct_action links
    :param params_input: Parameters input for direct_action links
    :param map_index: Map index for mapped tasks
    :param expiration_hours: Custom expiration time in hours
    :param session: Database session (required to get task instance UUID)
    :return: Base64-encoded token
    """
    requires_hitl_shared_links_enabled()

    if link_type == "direct_action" and not action:
        raise ValueError("Action is required for direct_action-type links")

    if session is None:
        raise ValueError("Database session is required to generate shared link token")

    if expiration_hours is None:
        expiration_hours = conf.getint("api", "hitl_shared_link_expiration_hours", fallback=24)

    expires_at = timezone.utcnow() + timedelta(hours=expiration_hours)

    # Get the task instance to use its UUID
    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
        try_number=try_number,
    )

    token_data = {
        "task_instance_uuid": str(task_instance.id),  # Use existing task instance ID
        "type": link_type,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "try_number": try_number,
        "map_index": map_index,
        "action": action,
        "chosen_options": chosen_options,
        "params_input": params_input,
        "expires_at": expires_at.isoformat(),
    }

    token_json = json.dumps(token_data, sort_keys=True)
    token_bytes = token_json.encode("utf-8")
    token = base64.urlsafe_b64encode(token_bytes).decode("utf-8")

    return token


def validate_shared_link_token(token: str) -> dict[str, Any]:
    """
    Validate and decode a shared link token.

    :param token: Base64-encoded token
    :return: Decoded token data
    :raises ValueError: If token is invalid or expired
    """
    requires_hitl_shared_links_enabled()

    try:
        token_bytes = base64.urlsafe_b64decode(token)
        token_json = token_bytes.decode("utf-8")
        token_data = json.loads(token_json)

        # Validate required fields
        required_fields = [
            "task_instance_uuid",
            "type",
            "dag_id",
            "dag_run_id",
            "task_id",
            "try_number",
            "expires_at",
        ]
        for field in required_fields:
            if field not in token_data:
                raise ValueError(f"Missing required field: {field}")

        # Check expiration
        expires_at = datetime.fromisoformat(token_data["expires_at"])
        if timezone.utcnow() > expires_at:
            raise ValueError("Token has expired")

        return token_data

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        raise ValueError(f"Invalid token: {e}")


def service_generate_shared_link(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int,
    link_type: str = "direct_action",
    action: str | None = None,
    chosen_options: list[str] | None = None,
    params_input: dict[str, Any] | None = None,
    map_index: int | None = None,
    expiration_hours: int | None = None,
    base_url: str | None = None,
    session: Session | None = None,
) -> dict[str, Any]:
    """
    Generate a shared link for HITL tasks.

    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID
    :param try_number: Try number for the task
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
    requires_hitl_shared_links_enabled()

    if session is None:
        raise ValueError("Database session is required to generate shared link")

    if base_url is None:
        base_url = conf.get("api", "base_url")
        if not base_url:
            raise ValueError("API base_url is not configured")

    token = generate_shared_link_token(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=try_number,
        link_type=link_type,
        action=action,
        chosen_options=chosen_options,
        params_input=params_input,
        map_index=map_index,
        expiration_hours=expiration_hours,
        session=session,
    )

    if link_type == "direct_action":
        url_path = "/api/v2/hitl-shared-links/execute"
    else:
        url_path = "/api/v2/hitl-shared-links/redirect"

    link_url = f"{base_url.rstrip('/')}{url_path}?token={token}"

    token_data = validate_shared_link_token(token)
    expires_at = datetime.fromisoformat(token_data["expires_at"])

    return {
        "url": link_url,
        "expires_at": expires_at.isoformat(),
        "link_type": link_type,
        "action": action,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "try_number": try_number,
        "map_index": map_index,
        "task_instance_uuid": token_data["task_instance_uuid"],
    }


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
    requires_hitl_shared_links_enabled()

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
    requires_hitl_shared_links_enabled()

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
