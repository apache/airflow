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

from airflow.configuration import conf
from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.smtp.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    import jinja2

    from airflow.sdk.definitions.context import Context


class SmtpNotifier(BaseNotifier):
    """
    SMTP Notifier.

    Accepts keyword arguments. The only required arguments are `from_email` and `to`. Examples:

    .. code-block:: python

        EmptyOperator(task_id="task", on_failure_callback=SmtpNotifier(from_email=None, to="myemail@myemail.com"))

        EmptyOperator(
            task_id="task",
            on_failure_callback=SmtpNotifier(
                from_email="myemail@myemail.com",
                to="myemail@myemail.com",
                subject="Task {{ ti.task_id }} failed",
            ),
        )

    Default template can be overridden via the following provider configuration data:
        - templated_email_subject_path
        - templated_html_content_path


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
        *,
        template: str | None = None,
    ):
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
        # If html_content is passed, prioritize it. Otherwise, if template is passed, use
        # it to populate html_content. Else, fall back to defaults defined in settings
        self.html_content: str | None = None
        if html_content is not None:
            self.html_content = html_content
        elif template is not None:
            self.html_content = Path(template).read_text()
        elif not AIRFLOW_V_3_0_PLUS:
            self.html_content = conf.get(
                "smtp",
                "templated_html_content_path",
                fallback=None,
            )

        if self.from_email is None and not AIRFLOW_V_3_0_PLUS:
            self.from_email = conf.get("smtp", "smtp_mail_from", fallback=None)

        if self.subject is None and not AIRFLOW_V_3_0_PLUS:
            self.subject = conf.get("smtp", "templated_email_subject_path", fallback=None)

    @cached_property
    def hook(self) -> SmtpHook:
        """Smtp Events Hook."""
        return SmtpHook(smtp_conn_id=self.smtp_conn_id)

    @cached_property
    def _connection_from_email(self) -> str | None:
        return self.hook.from_email

    @cached_property
    def _connection_subject_template(self) -> str | None:
        return self.hook.subject_template

    @cached_property
    def _connection_html_content_template(self) -> str | None:
        return self.hook.html_content_template

    def _render_fields(
        self,
        fields: Iterable[str],
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        dag = context["dag"]
        if not jinja_env:
            jinja_env = self.get_template_env(dag=dag)
        self._do_render_template_fields(self, fields, context, jinja_env, set())

    def notify(self, context):
        """Send a email via smtp server."""
        with self.hook as smtp:
            fields_to_re_render = []
            if self.from_email is None:
                if self._connection_from_email:
                    self.from_email = self._connection_from_email
                else:
                    if AIRFLOW_V_3_0_PLUS:
                        raise ValueError(
                            "you must provide from_email argument, or set a default one in the connection"
                        )
                    else:
                        raise ValueError(
                            "you must provide from_email argument, or set a default one in the configuration or the connection"
                        )
                fields_to_re_render.append("from_email")
            if self.subject is None:
                if self._connection_subject_template:
                    subject_template_path = Path(self._connection_subject_template).as_posix()
                else:
                    subject_template_path = (
                        Path(__file__).parent / "templates" / "email_subject.jinja2"
                    ).as_posix()
                self.subject = Path(subject_template_path).read_text().replace("\n", "").strip()
                fields_to_re_render.append("subject")
            if self.html_content is None:
                if self._connection_html_content_template:
                    default_html_content_template_path = Path(
                        self._connection_html_content_template
                    ).as_posix()
                else:
                    default_html_content_template_path = (
                        Path(__file__).parent / "templates" / "email.html"
                    ).as_posix()
                self.html_content = Path(default_html_content_template_path).read_text()
                fields_to_re_render.append("html_content")
            if fields_to_re_render:
                self._render_fields(fields_to_re_render, context)
            smtp.send_email_smtp(
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
