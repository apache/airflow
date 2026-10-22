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
"""Deprecated: the email utilities moved to :mod:`airflow.providers.smtp.utils.emailer`."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.utils.deprecation_tools import add_deprecated_classes

if TYPE_CHECKING:
    # Lets type checkers resolve the deprecated names, which only exist at runtime via __getattr__.
    from airflow.providers.smtp.utils.emailer import (
        build_mime_message as build_mime_message,
        get_email_address_list as get_email_address_list,
        send_email as send_email,
        send_email_smtp as send_email_smtp,
        send_mime_email as send_mime_email,
    )

add_deprecated_classes(
    {
        __name__: {
            "send_email": "airflow.providers.smtp.utils.emailer.send_email",
            "send_email_smtp": "airflow.providers.smtp.utils.emailer.send_email_smtp",
            "build_mime_message": "airflow.providers.smtp.utils.emailer.build_mime_message",
            "send_mime_email": "airflow.providers.smtp.utils.emailer.send_mime_email",
            "get_email_address_list": "airflow.providers.smtp.utils.emailer.get_email_address_list",
        },
    },
    package=__name__,
)
