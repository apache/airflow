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

import unittest
from unittest.mock import MagicMock

from sqlalchemy.exc import OperationalError

from airflow.models.dagwarning import DagWarning


class TestDagWarning(unittest.TestCase):
    def setUp(self):
        self.session_mock = MagicMock()
        self.delete_mock = MagicMock()
        self.session_mock.query.return_value.filter.return_value.delete = self.delete_mock

    def test_purge_inactive_dag_warnings(self):
        """
        Test that the purge_inactive_dag_warnings method calls the delete method once
        """
        DagWarning.purge_inactive_dag_warnings(self.session_mock)

        self.delete_mock.assert_called_once()

    def test_retry_purge_inactive_dag_warnings(self):
        """
        Test that the purge_inactive_dag_warnings method calls the delete method twice
        if the query throws an operationalError on the first call and works on the second attempt
        """
        self.delete_mock.side_effect = [OperationalError(None, None, "database timeout"), None]

        DagWarning.purge_inactive_dag_warnings(self.session_mock)

        # Assert that the delete method was called twice
        self.assertEqual(self.delete_mock.call_count, 2)
