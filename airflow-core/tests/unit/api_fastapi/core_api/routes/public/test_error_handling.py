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

from unittest import mock

import pytest
from fastapi import status

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


class TestDagBagErrorHandling:
    """Unit tests for error handling when using dag_bag.get_dag."""

    def test_get_dag_import_error(self, test_client):
        """Test error handling when dag_bag.get_dag raises ImportError."""
        # Mock the dag_bag.get_dag method to raise ImportError
        with mock.patch.object(
            test_client.app.state.dag_bag, "get_dag", side_effect=ImportError("Failed to import module")
        ):
            response = test_client.get("/dags/test_dag")
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "Failed to parse DAG" in response.json()["detail"]

    def test_get_dag_syntax_error(self, test_client):
        """Test error handling when dag_bag.get_dag raises SyntaxError."""
        # Mock the dag_bag.get_dag method to raise SyntaxError
        with mock.patch.object(
            test_client.app.state.dag_bag, "get_dag", side_effect=SyntaxError("Invalid syntax")
        ):
            response = test_client.get("/dags/test_dag")
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "Failed to parse DAG" in response.json()["detail"]

    def test_get_dag_generic_exception(self, test_client):
        """Test error handling when dag_bag.get_dag raises a generic Exception."""
        # Mock the dag_bag.get_dag method to raise a generic Exception
        with mock.patch.object(
            test_client.app.state.dag_bag, "get_dag", side_effect=Exception("Unexpected error")
        ):
            response = test_client.get("/dags/test_dag")
            assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
            assert "An unexpected error occurred" in response.json()["detail"]

    def test_get_dag_not_found(self, test_client):
        """Test error handling when dag_bag.get_dag returns None."""
        # Mock the dag_bag.get_dag method to return None
        with mock.patch.object(test_client.app.state.dag_bag, "get_dag", return_value=None):
            response = test_client.get("/dags/test_dag")
            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "was not found" in response.json()["detail"]
