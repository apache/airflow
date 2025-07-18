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

from airflow.sdk.bases.operator import DB_SAFE_MAXIMUM, DB_SAFE_MINIMUM, db_safe_priority


def test_db_safe_priority():
    """Test the db_safe_priority function."""
    assert db_safe_priority(1) == 1
    assert db_safe_priority(-1) == -1
    assert db_safe_priority(9999999999) == DB_SAFE_MAXIMUM
    assert db_safe_priority(-9999999999) == DB_SAFE_MINIMUM


def test_db_safe_constants():
    """Test the database safe constants."""
    assert DB_SAFE_MINIMUM == -2147483648
    assert DB_SAFE_MAXIMUM == 2147483647
