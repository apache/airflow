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
"""Utilities for Human-in-the-Loop (HITL) shared links."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import select

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone

log = structlog.get_logger(__name__)


class HITLSharedLinkManager:
    """Manager for HITL shared links with token generation and verification."""

    def __init__(self):
        self.default_expiration_hours = conf.getint("api", "hitl_shared_link_expiration_hours", fallback=24)

    def is_enabled(self) -> bool:
        """Check if HITL shared links are enabled."""
        return conf.getboolean("api", "hitl_enable_shared_links", fallback=False)

    def _get_task_instance(
        self,
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
            raise ValueError(
                f"Task instance not found: {dag_id}/{dag_run_id}/{task_id}"
                + (f"/{map_index}" if map_index is not None else "")
                + f" (try_number={try_number})"
            )

        return task_instance

    def generate_link(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        map_index: int | None = None,
        link_type: str = "direct_action",
        action: str | None = None,
        chosen_options: list[str] | None = None,
        params_input: dict[str, Any] | None = None,
        base_url: str | None = None,
        expiration_hours: int | None = None,
        session: Session | None = None,
    ) -> dict[str, Any]:
        """
        Generate a shared link for HITL task.

        :param dag_id: DAG ID
        :param dag_run_id: DAG run ID
        :param task_id: Task ID
        :param try_number: Try number for the task
        :param map_index: Map index for mapped tasks
        :param link_type: Type of link ('ui_redirect' or 'direct_action')
        :param action: Action to perform (for direct_action links)
        :param chosen_options: Chosen options for direct_action links
        :param params_input: Parameters input for direct_action links
        :param base_url: Base URL for the link
        :param expiration_hours: Custom expiration time in hours
        :param session: Database session (required to get task instance UUID)
        :return: Link data including URL and metadata
        """
        if not self.is_enabled():
            raise ValueError("HITL shared links are not enabled")

        if link_type == "direct_action" and not action:
            raise ValueError("Action is required for direct_action-type links")

        if session is None:
            raise ValueError("Database session is required to generate shared link")

        if expiration_hours is None:
            expiration_hours = self.default_expiration_hours

        expires_at = timezone.utcnow() + timedelta(hours=expiration_hours)

        # Get the task instance to use its UUID
        task_instance = self._get_task_instance(
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

        if base_url is None:
            base_url = conf.get("api", "base_url")
            if not base_url:
                raise ValueError("API base_url is not configured")

        if link_type == "direct_action":
            url_path = "/api/v2/hitl-shared-links/execute"
        else:
            url_path = "/api/v2/hitl-shared-links/redirect"

        link_url = f"{base_url.rstrip('/')}{url_path}?token={token}"

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
            "task_instance_uuid": str(task_instance.id),
        }

    def verify_link(self, token: str) -> dict[str, Any]:
        """
        Verify and decode a shared link token.

        :param token: Base64-encoded token
        :return: Decoded token data
        :raises ValueError: If token is invalid or expired
        """
        if not self.is_enabled():
            raise ValueError("HITL shared links are not enabled")

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

    def generate_redirect_link(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        map_index: int | None = None,
        base_url: str | None = None,
        expiration_hours: int | None = None,
        session: Session | None = None,
    ) -> dict[str, Any]:
        """
        Generate a redirect link that opens the HITL task in the Airflow UI.

        :param dag_id: DAG ID
        :param dag_run_id: DAG run ID
        :param task_id: Task ID
        :param try_number: Try number for the task
        :param map_index: Map index for mapped tasks
        :param base_url: Base URL for the link
        :param expiration_hours: Custom expiration time in hours
        :param session: Database session (required to get task instance UUID)
        :return: Link data including URL and metadata
        """
        return self.generate_link(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
            link_type="ui_redirect",
            action=None,
            chosen_options=None,
            params_input=None,
            base_url=base_url,
            expiration_hours=expiration_hours,
            session=session,
        )

    def generate_action_link(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        action: str,
        chosen_options: list[str] | None = None,
        params_input: dict[str, Any] | None = None,
        map_index: int | None = None,
        base_url: str | None = None,
        expiration_hours: int | None = None,
        session: Session | None = None,
    ) -> dict[str, Any]:
        """
        Generate an action link that directly performs an action.

        :param dag_id: DAG ID
        :param dag_run_id: DAG run ID
        :param task_id: Task ID
        :param try_number: Try number for the task
        :param action: Action to perform (e.g., 'approve', 'reject')
        :param chosen_options: Chosen options for the action
        :param params_input: Parameters input for the action
        :param map_index: Map index for mapped tasks
        :param base_url: Base URL for the link
        :param expiration_hours: Custom expiration time in hours
        :param session: Database session (required to get task instance UUID)
        :return: Link data including URL and metadata
        """
        return self.generate_link(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
            link_type="direct_action",
            action=action,
            chosen_options=chosen_options,
            params_input=params_input,
            base_url=base_url,
            expiration_hours=expiration_hours,
            session=session,
        )


# Global instance for easy access
hitl_shared_link_manager = HITLSharedLinkManager()
