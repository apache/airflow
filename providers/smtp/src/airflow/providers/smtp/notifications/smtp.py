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
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.smtp.version_compat import AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class SmtpNotifier(BaseNotifier):
    """
    SMTP Notifier.

    Accepts keyword arguments. The only required arguments are `from_email` and `to`. Examples:

    .. code-block:: python

        EmptyOperator(task_id="task", on_failure_callback=SmtpNotifier(from_email=None, to="my@mail.com"))

        EmptyOperator(
            task_id="task",
            on_failure_callback=SmtpNotifier(
                from_email="myemail@myemail.com",
                to="myemail@myemail.com",
                subject="Task {{ ti.task_id }} failed",
            ),
        )

    You can define a default template for subject and html_content in the SMTP connection configuration.

    :param smtp_conn_id: The :ref:`smtp connection id <howto/connection:smtp>`
        that contains the information used to authenticate the client.
    """

    template_fields = (
        "from_email",
        "to",
        "subject",
        "html_content",
        "files",
        "cc",
        "bcc",
        "mime_subtype",
        "mime_charset",
        "custom_headers",
    )

    def __init__(
        self,
        to: str | Iterable[str],
        from_email: str | None = None,
        subject: str | None = None,
        html_content: str | None = None,
        files: list[str] | None = None,
        cc: str | Iterable[str] | None = None,
        bcc: str | Iterable[str] | None = None,
        mime_subtype: str = "mixed",
        mime_charset: str = "utf-8",
        custom_headers: dict[str, Any] | None = None,
        smtp_conn_id: str = SmtpHook.default_conn_name,
        auth_type: str = "basic",
        *,
        template: str | None = None,
        **kwargs,
    ):
        if AIRFLOW_V_3_1_PLUS:
            #  Support for passing context was added in 3.1.0
            super().__init__(**kwargs)
        else:
            super().__init__()
        self.smtp_conn_id = smtp_conn_id
        self.from_email = from_email
        self.to = to
        self.files = files
        self.cc = cc
        self.bcc = bcc
        self.mime_subtype = mime_subtype
        self.mime_charset = mime_charset
        self.custom_headers = custom_headers
        self.subject = subject
        self.html_content = html_content
        self.auth_type = auth_type
        if self.html_content is None and template is not None:
            self.html_content = self._read_template(template)

    @staticmethod
    def _read_template(template_path: str) -> str:
        return Path(template_path).read_text().replace("\n", "").strip()

    @cached_property
    def hook(self) -> SmtpHook:
        """Smtp Events Hook."""
        return SmtpHook(smtp_conn_id=self.smtp_conn_id, auth_type=self.auth_type)

    def _build_email_content(self, smtp: SmtpHook, context: Context):
        fields_to_re_render = []
        if self.from_email is None:
            if smtp.from_email is not None:
                self.from_email = smtp.from_email
            else:
                raise ValueError("You should provide `from_email` or define it in the connection")
            fields_to_re_render.append("from_email")
        if self.subject is None:
            smtp_default_templated_subject_path: str
            if smtp.subject_template:
                smtp_default_templated_subject_path = smtp.subject_template
            else:
                smtp_default_templated_subject_path = (
                    Path(__file__).parent / "templates" / "email_subject.jinja2"
                ).as_posix()
            self.subject = self._read_template(smtp_default_templated_subject_path)
            fields_to_re_render.append("subject")
        if self.html_content is None:
            smtp_default_templated_html_content_path: str
            if smtp.html_content_template:
                smtp_default_templated_html_content_path = smtp.html_content_template
            else:
                smtp_default_templated_html_content_path = (
                    Path(__file__).parent / "templates" / "email.html"
                ).as_posix()
            self.html_content = self._read_template(smtp_default_templated_html_content_path)
            fields_to_re_render.append("html_content")
        if fields_to_re_render:
            jinja_env = self.get_template_env(dag=context.get("dag"))
            self._do_render_template_fields(self, fields_to_re_render, context, jinja_env, set())

    def notify(self, context: Context):
        """Send a email via smtp server."""
        with self.hook as smtp_hook:
            self._build_email_content(smtp_hook, context)
            smtp_hook.send_email_smtp(
                smtp_conn_id=self.smtp_conn_id,
                from_email=self.from_email,
                to=self.to,
                subject=self.subject,
                html_content=self.html_content,
                files=self.files,
                cc=self.cc,
                bcc=self.bcc,
                mime_subtype=self.mime_subtype,
                mime_charset=self.mime_charset,
                custom_headers=self.custom_headers,
            )

    async def async_notify(self, context: Context):
        """Send a email via smtp server (async)."""
        async with self.hook as smtp_hook:
            self._build_email_content(smtp_hook, context)
            await smtp_hook.asend_email_smtp(
                smtp_conn_id=self.smtp_conn_id,
                from_email=self.from_email,
                to=self.to,
                subject=self.subject,
                html_content=self.html_content,
                files=self.files,
                cc=self.cc,
                bcc=self.bcc,
                mime_subtype=self.mime_subtype,
                mime_charset=self.mime_charset,
                custom_headers=self.custom_headers,
            )


send_smtp_notification = SmtpNotifier
