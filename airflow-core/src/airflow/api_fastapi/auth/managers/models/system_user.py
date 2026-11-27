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

import attrs

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser


@attrs.define
class SystemUser(BaseUser):
    """
    Represents a trusted Airflow system process.

    Used for internal process-to-API authentication when code running inside
    trusted Airflow components (DAG processor, scheduler, worker, triggerer)
    needs to call the Core REST API without human user credentials.
    """

    process_type: str

    def get_id(self) -> str:
        return f"airflow:process:{self.process_type}"

    def get_name(self) -> str:
        return f"System ({self.process_type})"
