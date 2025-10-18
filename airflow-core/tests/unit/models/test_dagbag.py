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

from unittest.mock import MagicMock, patch
from airflow.models.dagbag import DBDagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.serialization.serialized_objects import SerializedDAG

pytestmark = pytest.mark.db_test

# This file previously contained tests for DagBag functionality, but those tests
# have been moved to airflow-core/tests/unit/dag_processing/test_dagbag.py to match
# the source code reorganization where DagBag moved from models to dag_processing.
#
# Tests for models-specific functionality (DBDagBag, DagPriorityParsingRequest, etc.)
# would remain in this file, but currently no such tests exist.


class TestDBDagBag:
    def setup_method(self):
        self.db_dag_bag = DBDagBag()
        self.session = MagicMock()

    def test__read_dag_stores_and_returns_dag(self):
        """It should store the SerializedDagModel in _dags and return the dag."""
        mock_dag = MagicMock(spec=SerializedDAG)
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_serdag.dag = mock_dag
        mock_serdag.dag_version_id = "v1"

        result = self.db_dag_bag._read_dag(mock_serdag)

        assert result == mock_dag
        assert self.db_dag_bag._dags["v1"] == mock_serdag
        assert mock_serdag.load_op_links is True

    def test__read_dag_returns_none_when_no_dag(self):
        """It should return None and not modify _dags when no DAG is present."""
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_serdag.dag = None
        mock_serdag.dag_version_id = "v1"

        result = self.db_dag_bag._read_dag(mock_serdag)

        assert result is None
        assert "v1" not in self.db_dag_bag._dags

    def test_get_dag_model_from_cache(self):
        """It should return the cached SerializedDagModel if already loaded."""
        mock_serdag = MagicMock(spec=SerializedDagModel)
        self.db_dag_bag._dags["v1"] = mock_serdag

        result = self.db_dag_bag.get_dag_model("v1", session=self.session)

        assert result == mock_serdag
        # session.get should not be called when cached
        self.session.get.assert_not_called()

    def test_get_dag_model_from_db(self):
        """It should fetch from the DB if not cached."""
        self.db_dag_bag._dags["v1"] = MagicMock(spec=SerializedDagModel)
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_dag_version = MagicMock()
        mock_dag_version.serialized_dag = mock_serdag
        self.session.get.return_value = mock_dag_version

        result = self.db_dag_bag.get_dag_model("v2", session=self.session)

        assert result == mock_serdag
        self.session.get.assert_called_once()

    def test_get_dag_model_returns_none_when_not_found(self):
        """It should return None if version_id not found in DB."""
        self.session.get.return_value = None

        result = self.db_dag_bag.get_dag_model("v1", session=self.session)

        assert result is None

    def test_get_dag_calls_get_dag_model_and__read_dag(self):
        """It should call get_dag_model and then _read_dag."""
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_dag = MagicMock(spec=SerializedDAG)

        with (
            patch.object(self.db_dag_bag, "get_dag_model", return_value=mock_serdag) as mock_get_model,
            patch.object(self.db_dag_bag, "_read_dag", return_value=mock_dag) as mock_read,
        ):
            result = self.db_dag_bag.get_dag("v1", session=self.session)

        mock_get_model.assert_called_once_with(version_id="v1", session=self.session)
        mock_read.assert_called_once_with(mock_serdag)
        assert result == mock_dag

    def test_get_dag_returns_none_when_model_missing(self):
        """It should return None if no SerializedDagModel found."""
        with patch.object(self.db_dag_bag, "get_dag_model", return_value=None):
            result = self.db_dag_bag.get_dag("v1", session=self.session)
        assert result is None
