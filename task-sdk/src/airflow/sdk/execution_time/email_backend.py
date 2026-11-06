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

from typing import TYPE_CHECKING, Any, Protocol

from airflow.sdk.bases.notifier import BaseNotifier
from airflow.sdk.configuration import conf
from airflow.sdk.exceptions import AirflowConfigException

if TYPE_CHECKING:
    from collections.abc import Iterable

    from airflow.sdk.definitions.context import Context

_DEFAULT_EMAIL_BACKEND = "airflow.utils.email.send_email_smtp"


class _ErrorEmailNotifier(Protocol):
    """
    Constructor contract shared by the notifiers used for failure and retry alerts.

    ``BaseNotifier`` itself does not describe this -- its ``__init__`` takes only ``context`` --
    so the shared shape is spelled out here to keep the call site type-checked.
    """

    def __call__(
        self,
        to: str | Iterable[str],
        from_email: str | None = ...,
        subject: str | None = ...,
        html_content: str | None = ...,
    ) -> BaseNotifier: ...


class _LegacyEmailBackendNotifier(BaseNotifier):
    """
    Adapter that exposes a legacy ``[email] email_backend`` callable as a notifier.

    Before failure and retry alerts were routed through ``BaseNotifier`` subclasses, deployments
    configured them through ``[email] email_backend`` -- a callable with the
    ``airflow.utils.email.send_email`` signature, such as the Amazon SES or SendGrid senders.
    This adapter renders the standard email fields like any notifier, then loads and calls the
    configured backend, so existing ``email_backend`` setups keep working unchanged.

    The backend is resolved from config at notify time rather than imported statically, keeping
    the Task SDK free of a hard dependency on ``airflow.utils.email`` (which lives in
    ``airflow-core``).
    """

    template_fields = ("to", "from_email", "subject", "html_content")

    def __init__(
        self,
        to: str | Iterable[str],
        from_email: str | None = None,
        subject: str | None = None,
        html_content: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.to = to
        self.from_email = from_email
        self.subject = subject
        self.html_content = html_content

    def notify(self, context: Context) -> None:
        backend = conf.getimport("email", "email_backend", fallback=_DEFAULT_EMAIL_BACKEND)
        if backend is None:
            raise AirflowConfigException("`[email] email_backend` is not configured")
        backend(
            self.to,
            self.subject,
            self.html_content,
            conn_id=conf.get("email", "email_conn_id", fallback=None),
            from_email=self.from_email,
        )
