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

from airflow.models.pool import Pool
from airflow.utils.session import create_session
from tests.test_utils.db import clear_db_pools


class TestPoolNameValidation:
    """Test pool name validation."""

    @pytest.fixture(autouse=True)
    def _clear_db(self):
        clear_db_pools()
        yield
        clear_db_pools()

    def test_valid_pool_names(self):
        """Test that valid pool names are accepted."""
        valid_names = [
            "valid_pool",
            "pool123",
            "Pool-Name",
            "pool.name",
            "pool_123.test-name",
            "ABC",
            "pool_123_test",
        ]
        with create_session() as session:
            for name in valid_names:
                # Should not raise any exception
                Pool.create_or_update_pool(name=name, slots=5, session=session)

    def test_invalid_pool_names(self):
        """Test that invalid pool names are rejected."""
        invalid_names = [
            "pool name",  # space
            "pool😎",  # emoji
            "pool@name",  # special char
            "pool#name",  # special char
        ]
        with create_session() as session:
            for name in invalid_names:
                with pytest.raises(
                    ValueError,
                    match="Pool name must only contain ASCII letters, numbers, underscores, dots, and dashes",
                ):
                    Pool.create_or_update_pool(name=name, slots=5, session=session)

    def test_pool_name_validation_error_message(self):
        """Test that validation error has clear message."""
        with create_session() as session:
            with pytest.raises(ValueError) as exc_info:
                Pool.create_or_update_pool(name="invalid pool name", slots=5, session=session)
            
            assert "Pool name must only contain ASCII letters" in str(exc_info.value)
            assert "Got: 'invalid pool name'" in str(exc_info.value)
