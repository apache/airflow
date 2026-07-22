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

from collections.abc import Iterable
from typing import Any
from unittest import mock

import pytest

from airflow.sdk.configuration import conf
from airflow.sdk.exceptions import AirflowConfigException
from airflow.sdk.execution_time.email_backend import _LegacyEmailBackendNotifier


def _legacy_email_backend(
    to: list[str] | Iterable[str],
    subject: str,
    html_content: str,
    files: list[str] | None = None,
    dryrun: bool = False,
    cc: str | Iterable[str] | None = None,
    bcc: str | Iterable[str] | None = None,
    mime_subtype: str = "mixed",
    mime_charset: str = "utf-8",
    conn_id: str | None = None,
    custom_headers: dict[str, Any] | None = None,
    **kwargs,
) -> None:
    """
    Spec for an ``[email] email_backend`` callable.

    Mirrors the ``airflow.utils.email.send_email`` signature so the autospec enforces the
    calling convention the notifier has to honour. Duplicated here rather than imported
    because the Task SDK must not depend on ``airflow-core``.
    """
    raise AssertionError("spec only; never called")


class TestLegacyEmailBackendNotifier:
    def test_notify_calls_configured_backend(self):
        """notify() loads the legacy backend from config and calls it with the standard fields."""
        backend = mock.create_autospec(_legacy_email_backend)
        notifier = _LegacyEmailBackendNotifier(
            to=["a@b.com"],
            from_email="from@x.com",
            subject="Subject",
            html_content="<p>body</p>",
        )
        with (
            mock.patch.object(conf, "getimport", autospec=True, return_value=backend) as getimport,
            mock.patch.object(conf, "get", autospec=True, return_value="my_conn"),
        ):
            notifier.notify(context={})

        getimport.assert_called_once_with(
            "email", "email_backend", fallback="airflow.utils.email.send_email_smtp"
        )
        backend.assert_called_once_with(
            ["a@b.com"],
            "Subject",
            "<p>body</p>",
            conn_id="my_conn",
            from_email="from@x.com",
        )

    def test_notify_raises_when_backend_unresolvable(self):
        """An empty/unloadable backend raises rather than silently doing nothing."""
        notifier = _LegacyEmailBackendNotifier(to="a@b.com")
        with mock.patch.object(conf, "getimport", autospec=True, return_value=None):
            with pytest.raises(AirflowConfigException):
                notifier.notify(context={})
