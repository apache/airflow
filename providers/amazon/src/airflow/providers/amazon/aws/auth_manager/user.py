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

from airflow.exceptions import AirflowOptionalProviderFeatureException

try:
    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
except ImportError:
    try:
        from airflow.auth.managers.models.base_user import BaseUser  # type: ignore[no-redef]
    except ImportError:
        raise AirflowOptionalProviderFeatureException(
            "Failed to import BaseUser. This feature is only available in Airflow versions >= 2.8.0"
        ) from None


class AwsAuthManagerUser(BaseUser):
    """
    User model for users managed by the AWS Auth Manager.

    :param user_id: The user ID.
    :param groups: The groups the user belongs to.
    :param username: The username of the user.
    :param email: The email of the user.
    """

    def __init__(
        self, *, user_id: str, groups: list[str], username: str | None = None, email: str | None = None
    ) -> None:
        self.user_id = user_id
        self.groups = groups
        self.username = username
        self.email = email

    def get_id(self) -> str:
        return self.user_id

    def get_name(self) -> str:
        return self.username or self.email or self.user_id

    def get_groups(self):
        return self.groups
