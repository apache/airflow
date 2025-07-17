#
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
"""Airflow module for email backend using AWS SES."""

from __future__ import annotations

from typing import Any

from airflow.providers.amazon.aws.hooks.ses import SesHook


def send_email(
    to: list[str] | str,
    subject: str,
    html_content: str,
    files: list | None = None,
    cc: list[str] | str | None = None,
    bcc: list[str] | str | None = None,
    mime_subtype: str = "mixed",
    mime_charset: str = "utf-8",
    conn_id: str = "aws_default",
    from_email: str | None = None,
    custom_headers: dict[str, Any] | None = None,
    **kwargs,
) -> None:
    """Email backend for SES."""
    if from_email is None:
        raise RuntimeError("The `from_email' configuration has to be set for the SES emailer.")
    hook = SesHook(aws_conn_id=conn_id)
    hook.send_email(
        mail_from=from_email,
        to=to,
        subject=subject,
        html_content=html_content,
        files=files,
        cc=cc,
        bcc=bcc,
        mime_subtype=mime_subtype,
        mime_charset=mime_charset,
        custom_headers=custom_headers,
    )
