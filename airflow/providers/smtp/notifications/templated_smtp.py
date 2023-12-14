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

from pathlib import Path
from typing import Iterable

from airflow.configuration import conf
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.settings import SMTP_DEFAULT_TEMPLATED_HTML_CONTENT_PATH, SMTP_DEFAULT_TEMPLATED_SUBJECT

# Read once and cache - is this desirable? If users edit local settings, their changes will not take effect
# On the other hand, if we put this in the constructor, then we need to read from disk on each instantiation
SMTP_DEFAULT_TEMPLATED_HTML_CONTENT = Path(SMTP_DEFAULT_TEMPLATED_HTML_CONTENT_PATH).read_text()


class TemplatedSmtpNotifier(SmtpNotifier):
    """Templated version of the SMTP Notifier.

    Accepts minimal arguments and uses a generic subject line / template that can be overridden
    by replacing the variable SMTP_DEFAULT_TEMPLATED_SUBJECT or
    SMTP_DEFAULT_TEMPLATED_HTML_CONTENT_PATH in airflow local settings.
    """

    def __init__(
        self,
        to: str | Iterable[str],
        **kwargs,
    ):
        from_email = kwargs.pop("from_email", conf.get("smtp", "smtp_mail_from"))
        subject = kwargs.pop("subject", SMTP_DEFAULT_TEMPLATED_SUBJECT).replace("\n", "").strip()
        html_content = SMTP_DEFAULT_TEMPLATED_HTML_CONTENT
        super().__init__(from_email=from_email, to=to, subject=subject, html_content=html_content, **kwargs)


send_templated_smtp_notification = TemplatedSmtpNotifier
