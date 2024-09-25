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

import json
import os

import pytest

from airflow.models import Connection
from airflow.utils.session import create_session

from dev.tests_common.test_utils.db import clear_db_connections
from dev.tests_common.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest

TOKEN = os.environ.get("DATAPREP_TOKEN")
EXTRA = {"token": TOKEN}


@pytest.mark.skipif(TOKEN is None, reason="Dataprep token not present")
class TestDataprepExampleDagsSystem(GoogleSystemTest):
    """
    System tests for Dataprep operators.
    It uses a real service and requires real data for test.
    """

    def setup_method(self):
        with create_session() as session:
            dataprep_conn_id = Connection(
                conn_id="dataprep_default",
                conn_type="dataprep",
                extra=json.dumps(EXTRA),
            )
            session.add(dataprep_conn_id)

    def teardown_method(self):
        clear_db_connections()

    def test_run_example_dag(self):
        self.run_dag(dag_id="example_dataprep", dag_folder=CLOUD_DAG_FOLDER)
