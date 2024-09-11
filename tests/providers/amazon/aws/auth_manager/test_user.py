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

import pytest

from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser


@pytest.fixture
def user():
    return AwsAuthManagerUser(user_id="user_id", groups=[])


class TestAwsAuthManagerUser:
    def test_get_id(self, user):
        assert user.get_id() == "user_id"

    def test_get_name_with_username(self, user):
        user.username = "username"
        assert user.get_name() == "username"

    def test_get_name_with_email(self, user):
        user.email = "email"
        assert user.get_name() == "email"

    def test_get_name_with_user_id(self, user):
        user.user_id = "user_id"
        assert user.get_name() == "user_id"

    def test_get_groups(self, user):
        assert user.get_groups() == []
