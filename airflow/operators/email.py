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
from __future__ import annotations

import warnings

from airflow.providers.smtp.operators.smtp import EmailOperator as ProviderEmailOperator
from airflow.utils.context import Context
from airflow.utils.email import _SmtpHook

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.smtp.operators.smtp` instead.",
    DeprecationWarning,
    stacklevel=2,
)


class EmailOperator(ProviderEmailOperator):
    """
    Sends an email.
    Deprecated: Please use `airflow.providers.smtp.operators.smtp.EmailOperator`.
    """

    def execute(self, context: Context):
        with _SmtpHook(smtp_conn_id=self.conn_id) as smtp_hook:
            return smtp_hook.send_email_smtp(
                to=self.to,
                subject=self.subject,
                html_content=self.html_content,
                from_email=self.from_email,
                files=self.files,
                cc=self.cc,
                bcc=self.bcc,
                mime_subtype=self.mime_subtype,
                mime_charset=self.mime_charset,
                conn_id=self.conn_id,
                custom_headers=self.custom_headers,
            )
