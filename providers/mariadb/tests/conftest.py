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

"""Configuration for MariaDB provider tests."""

import pytest


@pytest.fixture
def mariadb_connection():
    """Fixture for MariaDB connection configuration."""
    return {
        "conn_id": "mariadb_default",
        "conn_type": "mariadb",
        "host": "localhost",
        "port": 3306,
        "schema": "test_db",
        "login": "test_user",
        "password": "test_pass",
    }
