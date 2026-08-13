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
"""Airflow module for email backend using Microsoft Graph."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from airflow.providers.microsoft.azure.hooks.msgraph import MSGraphMailHook

if TYPE_CHECKING:
    from collections.abc import Iterable

log = logging.getLogger(__name__)


def send_email(
    to: str | Iterable[str],
    subject: str,
    html_content: str,
    files: list[str] | None = None,
    dryrun: bool = False,
    cc: str | Iterable[str] | None = None,
    bcc: str | Iterable[str] | None = None,
    mime_subtype: str = "mixed",
    mime_charset: str = "utf-8",
    conn_id: str | None = None,
    from_email: str | None = None,
    custom_headers: dict[str, Any] | None = None,
    **kwargs,
) -> None:
    """Email backend for Microsoft Graph."""
    if not from_email:
        raise ValueError(
            "The `from_email` configuration has to be set for the Microsoft Graph emailer, as it "
            "determines which mailbox the message is sent from."
        )

    if dryrun:
        log.info("Dryrun, not sending email with subject %r to %s", subject, to)
        return

    # ``mime_subtype`` and ``mime_charset`` are part of the email backend contract but have no
    # counterpart here: Microsoft Graph composes the MIME message itself from the JSON payload.
    hook = MSGraphMailHook(conn_id=conn_id or MSGraphMailHook.default_conn_name)
    hook.send_email(
        from_email=from_email,
        to=to,
        subject=subject,
        html_content=html_content,
        files=files,
        cc=cc,
        bcc=bcc,
        custom_headers=custom_headers,
    )
