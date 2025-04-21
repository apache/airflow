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

from airflow.providers.databricks.sensors.databricks import DatabricksSQLStatementsSensor

STATEMENT_ID = "statement_id"
TASK_ID = "task_id"
WAREHOUSE_ID = "warehouse_id"


class TestDatabricksSQLStatementsSensor:
    def test_init_statement(self):
        """Test initialization for traditional use-case (statement)."""
        op = DatabricksSQLStatementsSensor(
            task_id=TASK_ID, statement="SELECT * FROM test.test;", warehouse_id=WAREHOUSE_ID
        )

        assert op.statement == "SELECT * FROM test.test;"
        assert op.warehouse_id == WAREHOUSE_ID

    def test_init_statement_id(self):
        """Test initialization when a statement_id is passed, rather than a statement."""
        op = DatabricksSQLStatementsSensor(
            task_id=TASK_ID, statement_id=STATEMENT_ID, warehouse_id=WAREHOUSE_ID
        )

        assert op.statement_id == STATEMENT_ID
        assert op.warehouse_id == WAREHOUSE_ID

    def test_exec_success(self):
        pass
