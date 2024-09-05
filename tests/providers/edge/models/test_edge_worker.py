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

from typing import TYPE_CHECKING

import pytest

from airflow.providers.edge.models.edge_worker import (
    EdgeWorker,
    EdgeWorkerModel,
    EdgeWorkerVersionException,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


class TestEdgeWorker:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, session: Session):
        session.query(EdgeWorkerModel).delete()

    def test_assert_version(self):
        from airflow import __version__ as airflow_version
        from airflow.providers.edge import __version__ as edge_provider_version

        with pytest.raises(EdgeWorkerVersionException):
            EdgeWorker.assert_version({})

        with pytest.raises(EdgeWorkerVersionException):
            EdgeWorker.assert_version({"airflow_version": airflow_version})

        with pytest.raises(EdgeWorkerVersionException):
            EdgeWorker.assert_version({"edge_provider_version": edge_provider_version})

        with pytest.raises(EdgeWorkerVersionException):
            EdgeWorker.assert_version(
                {"airflow_version": "1.2.3", "edge_provider_version": edge_provider_version}
            )

        with pytest.raises(EdgeWorkerVersionException):
            EdgeWorker.assert_version(
                {"airflow_version": airflow_version, "edge_provider_version": "2023.10.07"}
            )

        EdgeWorker.assert_version(
            {"airflow_version": airflow_version, "edge_provider_version": edge_provider_version}
        )
