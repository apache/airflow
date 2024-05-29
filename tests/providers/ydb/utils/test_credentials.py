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

import re
from datetime import datetime, timedelta
from airflow.utils import timezone
from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest
import responses
from responses import matchers

from airflow.models.connection import Connection
from airflow.providers.ydb.utils.credentials import get_credentials_from_connection
TEST_ENDPOINT="my_endpoint"
TEST_DATABASE="/my_db"

@patch("ydb.StaticCredentials")
def test_static_creds(mock):
    mock.return_value = 1
    c = Connection(conn_type="ydb", host="localhost", login="my_login")
    credentials = get_credentials_from_connection(TEST_ENDPOINT, TEST_DATABASE, c, {})
    assert isinstance(credentials, int)