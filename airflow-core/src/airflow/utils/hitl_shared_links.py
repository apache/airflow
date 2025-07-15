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
import hashlib
import hmac
import json
from datetime import datetime, timedelta
from typing import TypedDict
from urllib.parse import urlencode

import structlog

from airflow.api_fastapi.core_api.datamodels.hitl import HITLSharedLinkPayload
from airflow.configuration import conf
from airflow.utils import timezone

log = structlog.get_logger(__name__)


class HITLSharedLinkResponse(TypedDict):
    """Response data for generated HITL shared links."""

    url: str
    expires_at: str
    link_type: str
    action: str | None
    dag_id: str
    dag_run_id: str
    task_id: str
    try_number: int
    map_index: int | None


class HITLSharedLinkManager:
    """Manager for HITL shared links with token generation and verification."""

    def __init__(self):
        self.secret_key = conf.get("api", "secret_key")
        self.default_expiration_hours = conf.getint("api", "hitl_shared_link_expiration_hours")

    def is_enabled(self) -> bool:
        """Check if HITL shared links are enabled."""
        return conf.getboolean("api", "hitl_enable_shared_links", fallback=False)

    def _generate_signature(self, payload: str) -> str:
        """Generate HMAC signature for the payload."""
        if not self.secret_key:
            raise ValueError("API secret key is not configured")

        signature = hmac.new(
            self.secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
        ).digest()
        return base64.urlsafe_b64encode(signature).decode("utf-8")

    def _verify_signature(self, payload: str, signature: str) -> bool:
        """Verify HMAC signature for the payload."""
        expected_signature = self._generate_signature(payload)
        return hmac.compare_digest(expected_signature, signature)

    def generate_link(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        map_index: int | None = None,
        link_type: str = "action",
        action: str | None = None,
        base_url: str | None = None,
    ) -> HITLSharedLinkResponse:
        """
        Generate a shared link for HITL task.

        :param dag_id: DAG ID
        :param dag_run_id: DAG run ID
        :param task_id: Task ID
        :param try_number: Try number for the task
        :param map_index: Map index for mapped tasks
        :param link_type: Type of link ('action' or 'redirect')
        :param action: Action to perform (for action links)
        :param base_url: Base URL for the link
        """
        if not self.is_enabled():
            raise ValueError("HITL shared links are not enabled")

        if link_type == "action" and not action:
            raise ValueError("Action is required for action-type links")

        expires_at = timezone.utcnow() + timedelta(hours=self.default_expiration_hours)

        payload_data = HITLSharedLinkPayload(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
            link_type=link_type,
            action=action,
            expires_at=expires_at.isoformat(),
        )

        payload_str = payload_data.model_dump_json()
        signature = self._generate_signature(payload_str)

        encoded_payload = base64.urlsafe_b64encode(payload_str.encode("utf-8")).decode("utf-8")

        if base_url is None:
            base_url = conf.get("api", "base_url")
            if not base_url:
                raise ValueError("API base_url is not configured")

        if map_index is not None:
            url_path = f"/api/v2/hitl-details/share-link/{dag_id}/{dag_run_id}/{task_id}/{map_index}"
        else:
            url_path = f"/api/v2/hitl-details/share-link/{dag_id}/{dag_run_id}/{task_id}"

        query_params = {
            "payload": encoded_payload,
            "signature": signature,
        }

        link_url = f"{base_url.rstrip('/')}{url_path}?{urlencode(query_params)}"

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
        }

    def verify_link(self, payload: str, signature: str) -> HITLSharedLinkPayload:
        """
        Verify and decode a shared link.

        :param payload: Base64-encoded payload
        :param signature: HMAC signature

        :raises ValueError: If signature is invalid or link has expired
        """
        if not self.is_enabled():
            raise ValueError("HITL shared links are not enabled")

        try:
            payload_bytes = base64.urlsafe_b64decode(payload)
            payload_str = payload_bytes.decode("utf-8")

            if not self._verify_signature(payload_str, signature):
                raise ValueError("Invalid signature")

            payload_data = HITLSharedLinkPayload.model_validate_json(payload_str)

            expires_at = datetime.fromisoformat(payload_data.expires_at)
            if timezone.utcnow() > expires_at:
                raise ValueError("Link has expired")

            return payload_data

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise ValueError(f"Invalid payload: {e}")

    def generate_redirect_link(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        map_index: int | None = None,
        base_url: str | None = None,
    ) -> HITLSharedLinkResponse:
        """
        Generate a redirect link that opens the HITL task in the UI.

        :param dag_id: DAG ID
        :param dag_run_id: DAG run ID
        :param task_id: Task ID
        :param try_number: Try number for the task
        :param map_index: Map index for mapped tasks
        :param base_url: Base URL for the link
        """
        return self.generate_link(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
            link_type="redirect",
            action=None,
            base_url=base_url,
        )

    def generate_action_link(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        action: str,
        map_index: int | None = None,
        base_url: str | None = None,
    ) -> HITLSharedLinkResponse:
        """
        Generate an action link that directly performs an action.

        :param dag_id: DAG ID
        :param dag_run_id: DAG run ID
        :param task_id: Task ID
        :param try_number: Try number for the task
        :param action: Action to perform (e.g., 'approve', 'reject')
        :param map_index: Map index for mapped tasks
        :param base_url: Base URL for the link
        """
        return self.generate_link(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
            link_type="action",
            action=action,
            base_url=base_url,
        )


hitl_shared_link_manager = HITLSharedLinkManager()
