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

import logging
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from sqlalchemy.exc import InternalError, OperationalError

from airflow.utils.retries import retry_db_transaction

if TYPE_CHECKING:
    from sqlalchemy.exc import DBAPIError

pytestmark = pytest.mark.skip_if_database_isolation_mode


class TestRetries:
    def test_retry_db_transaction_with_passing_retries(self):
        """Test that retries can be passed to decorator"""
        mock_obj = mock.MagicMock()
        mock_session = mock.MagicMock()
        op_error = OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

        @retry_db_transaction(retries=2)
        def test_function(session):
            session.execute("select 1")
            mock_obj(2)
            raise op_error

        with pytest.raises(OperationalError):
            test_function(session=mock_session)

        assert mock_obj.call_count == 2

    @pytest.mark.db_test
    @pytest.mark.parametrize("excection_type", [OperationalError, InternalError])
    def test_retry_db_transaction_with_default_retries(self, caplog, excection_type: type[DBAPIError]):
        """Test that by default 3 retries will be carried out"""
        mock_obj = mock.MagicMock()
        mock_session = mock.MagicMock()
        mock_rollback = mock.MagicMock()
        mock_session.rollback = mock_rollback
        db_error = excection_type(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

        @retry_db_transaction
        def test_function(session):
            session.execute("select 1")
            mock_obj(2)
            raise db_error

        caplog.set_level(logging.DEBUG, logger=self.__module__)
        caplog.clear()
        with pytest.raises(excection_type):
            test_function(session=mock_session)

        for try_no in range(1, 4):
            assert (
                "Running TestRetries.test_retry_db_transaction_with_default_retries.<locals>.test_function "
                f"with retries. Try {try_no} of 3" in caplog.messages
            )
        assert mock_session.execute.call_count == 3
        assert mock_rollback.call_count == 3
        mock_rollback.assert_has_calls([mock.call(), mock.call(), mock.call()])

    def test_retry_db_transaction_fails_when_used_in_function_without_retry(self):
        """Test that an error is raised when the decorator is used on a function without session arg"""

        with pytest.raises(ValueError, match=r"has no `session` argument"):

            @retry_db_transaction
            def test_function():
                print("hi")
                raise OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

    def test_retry_db_transaction_fails_when_session_not_passed(self):
        """Test that an error is raised when session is not passed to the function"""

        @retry_db_transaction
        def test_function(session):
            session.execute("select 1;")
            raise OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

        error_message = rf"session is a required argument for {test_function.__qualname__}"
        with pytest.raises(TypeError, match=error_message):
            test_function()
