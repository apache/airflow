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

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context


class CloudRunJobLoggingLink(BaseGoogleLink):
    """Helper class for constructing Cloud Run Job Logging link."""

    name = "Cloud Run Job Logging"
    key = "log_uri"

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        log_uri: str,
    ):
        task_instance.xcom_push(
            context,
            key=CloudRunJobLoggingLink.key,
            value=log_uri,
        )
