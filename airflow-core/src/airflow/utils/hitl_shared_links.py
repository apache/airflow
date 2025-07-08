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
"""Utilities for HITL shared links functionality."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlencode

from airflow.configuration import conf
from airflow.utils import timezone


class HITLSharedLinkManager:
    """Manager for HITL shared links functionality."""

    def __init__(self):
        self.secret_key = self._get_secret_key()
        self.expiration_hours = conf.getint("api", "hitl_shared_link_expiration_hours", fallback=24)

    def _get_secret_key(self) -> bytes:
        """Get the secret key for signing links."""
        secret_key = conf.get("api", "hitl_shared_link_secret_key", fallback="")
        if not secret_key:
            secret_key = "airflow-hitl-default-key"
        return secret_key.encode("utf-8")

    def is_enabled(self) -> bool:
        """Check if HITL shared links are enabled."""
        return conf.getboolean("api", "hitl_enable_shared_links", fallback=False)

    def generate_link(
        self,
        task_instance_id: str,
        action: str | None = None,
        expires_in_hours: int | None = None,
        link_type: str = "action",
    ) -> tuple[str, datetime]:
        """Generate a signed HITL shared link."""
        if not self.is_enabled():
            raise ValueError("HITL shared links are not enabled")

        expiration_hours = expires_in_hours or self.expiration_hours
        expires_at = timezone.utcnow() + timedelta(hours=expiration_hours)

        payload = {
            "ti_id": task_instance_id,
            "action": action,
            "link_type": link_type,
            "expires_at": expires_at.isoformat(),
        }

        signature = self._sign_payload(payload)

        payload_str = json.dumps(payload, separators=(",", ":"))
        payload_b64 = base64.urlsafe_b64encode(payload_str.encode("utf-8")).decode("utf-8")
        signature_b64 = base64.urlsafe_b64encode(signature).decode("utf-8")

        params = {
            "payload": payload_b64,
            "signature": signature_b64,
        }

        if link_type == "redirect":
            link_url = f"/hitl/shared/{task_instance_id}/redirect?{urlencode(params)}"
        else:
            link_url = f"/hitl/shared/{task_instance_id}?{urlencode(params)}"

        return link_url, expires_at

    def generate_redirect_link(
        self, task_instance_id: str, expires_in_hours: int | None = None
    ) -> tuple[str, datetime]:
        """Generate a redirect link that opens the Airflow UI for HITL interaction."""
        return self.generate_link(
            task_instance_id=task_instance_id,
            action=None,
            expires_in_hours=expires_in_hours,
            link_type="redirect",
        )

    def generate_action_link(
        self, task_instance_id: str, action: str, expires_in_hours: int | None = None
    ) -> tuple[str, datetime]:
        """Generate a direct action link that immediately performs the specified action."""
        return self.generate_link(
            task_instance_id=task_instance_id,
            action=action,
            expires_in_hours=expires_in_hours,
            link_type="action",
        )

    def verify_link(self, payload_b64: str, signature_b64: str) -> dict[str, Any]:
        """Verify and decode a HITL shared link."""
        try:
            payload_str = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
            signature = base64.urlsafe_b64decode(signature_b64.encode("utf-8"))

            payload = json.loads(payload_str)

            expected_signature = self._sign_payload(payload)
            if not hmac.compare_digest(signature, expected_signature):
                raise ValueError("Invalid signature")

            expires_at = datetime.fromisoformat(payload["expires_at"])
            if timezone.utcnow() > expires_at:
                raise ValueError("Link has expired")

            return payload

        except (ValueError, json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid link: {e}")

    def _sign_payload(self, payload: dict[str, Any]) -> bytes:
        """Sign a payload with HMAC."""
        payload_str = json.dumps(payload, separators=(",", ":"))
        return hmac.new(self.secret_key, payload_str.encode("utf-8"), hashlib.sha256).digest()


hitl_shared_link_manager = HITLSharedLinkManager()
