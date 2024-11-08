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

from functools import cached_property
from pathlib import Path
from typing import Any, Iterable

from airflow.configuration import conf
from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.smtp.hooks.smtp import SmtpHook


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
        # TODO: Move from_email to keyword parameter in next major release so that users do not
        # need to specify from_email. No argument here will lead to defaults from conf being used.
        from_email: str | None,
        to: str | Iterable[str],
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
        self.from_email = from_email or conf.get("smtp", "smtp_mail_from")
        self.to = to
        self.files = files
        self.cc = cc
        self.bcc = bcc
        self.mime_subtype = mime_subtype
        self.mime_charset = mime_charset
        self.custom_headers = custom_headers

        smtp_default_templated_subject_path = conf.get(
            "smtp",
            "templated_email_subject_path",
            fallback=(Path(__file__).parent / "templates" / "email_subject.jinja2").as_posix(),
        )
        self.subject = (
            subject or Path(smtp_default_templated_subject_path).read_text().replace("\n", "").strip()
        )
        # If html_content is passed, prioritize it. Otherwise, if template is passed, use
        # it to populate html_content. Else, fall back to defaults defined in settings
        if html_content is not None:
            self.html_content = html_content
        elif template is not None:
            self.html_content = Path(template).read_text()
        else:
            smtp_default_templated_html_content_path = conf.get(
                "smtp",
                "templated_html_content_path",
                fallback=(Path(__file__).parent / "templates" / "email.html").as_posix(),
            )
            self.html_content = Path(smtp_default_templated_html_content_path).read_text()

    @cached_property
    def hook(self) -> SmtpHook:
        """Smtp Events Hook."""
        return SmtpHook(smtp_conn_id=self.smtp_conn_id)

    def notify(self, context):
        """Send a email via smtp server."""
        with self.hook as smtp:
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
