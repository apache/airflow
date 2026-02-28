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

import pathlib
from unittest.mock import MagicMock, patch

import pytest
import structlog

from airflow.dag_processing.processor import DagFileParseRequest, _parse_file


@pytest.fixture
def mock_logger():
    return MagicMock(spec=structlog.typing.FilteringBoundLogger)


class TestProcessorKnownPools:
    def test_parse_file_passes_known_pools_to_dagbag(self, mock_logger):
        """Test that _parse_file passes known_pools to BundleDagBag."""
        known_pools = {"pool1", "pool2"}

        with patch("airflow.dag_processing.processor.BundleDagBag") as mock_dagbag_class:
            mock_dagbag_instance = MagicMock()
            mock_dagbag_instance.dags = {}
            mock_dagbag_instance.import_errors = {}
            mock_dagbag_class.return_value = mock_dagbag_instance

            request = DagFileParseRequest(
                file="/test/dag.py",
                bundle_path=pathlib.Path("/test"),
                bundle_name="test_bundle",
                known_pools=known_pools,
            )

            _parse_file(request, log=mock_logger)

            mock_dagbag_class.assert_called_once()
            call_kwargs = mock_dagbag_class.call_args.kwargs
            assert call_kwargs["known_pools"] == known_pools

    def test_dagbag_generates_warnings_for_unknown_pools(self, tmp_path):
        """Test that DagBag generates warnings when unknown pools are used."""
        # This test integration of DagBag logic with known_pools
        from airflow.dag_processing.dagbag import DagBag
        from airflow.models.dagwarning import DagWarningType

        dag_file = tmp_path / "test_dag_pool.py"
        dag_file.write_text("""
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG("test_pool_dag", start_date=datetime(2023, 1, 1), schedule=None):
    EmptyOperator(task_id="task1", pool="pool_one")
    EmptyOperator(task_id="task2", pool="pool_two")
""")

        known_pools = {"pool_one", "default_pool"}

        # We need to set bundle_path because BundleDagBag might be used or we use DagBag directly
        # Here we test DagBag logic directly as used in processor
        dagbag = DagBag(dag_folder=dag_file, known_pools=known_pools, include_examples=False)

        assert "test_pool_dag" in dagbag.dags
        warnings = dagbag.dag_warnings
        assert len(warnings) == 1
        warning = next(iter(warnings))
        assert warning.dag_id == "test_pool_dag"
        assert warning.warning_type == DagWarningType.NONEXISTENT_POOL
        assert "pool_two" in warning.message
        assert "pool_one" not in warning.message
