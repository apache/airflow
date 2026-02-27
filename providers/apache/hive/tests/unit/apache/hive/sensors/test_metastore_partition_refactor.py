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

from airflow.providers.apache.hive.sensors.metastore_partition import MetastorePartitionSensor


class TestMetastorePartitionSensor:
    def test_init(self):
        op = MetastorePartitionSensor(
            task_id="test_task",
            table="my_table",
            partition_name="ds=2023-01-01",
            schema="my_schema",
            mysql_conn_id="my_conn",
        )
        assert op.table == "my_table"
        assert op.partition_name == "ds=2023-01-01"
        assert op.schema == "my_schema"
        assert op.conn_id == "my_conn"
        assert op.sql == ""  # Initial dummy value

    @mock.patch("airflow.providers.common.sql.sensors.sql.SqlSensor._get_hook")
    def test_poke_sql_construction(self, mock_get_hook):
        op = MetastorePartitionSensor(
            task_id="test_task", table="my_table", partition_name="ds=2023-01-01", schema="my_schema"
        )

        # Mock hook behavior
        hook = mock.MagicMock()
        mock_get_hook.return_value = hook
        hook.get_records.return_value = [[1]]  # Return non-empty result

        context = {"ds": "2023-01-01"}  # Dummy context
        result = op.poke(context)

        assert result is True
        # Verify SQL construction
        expected_sql = """
            SELECT 'X'
            FROM PARTITIONS A0
            LEFT OUTER JOIN TBLS B0 ON A0.TBL_ID = B0.TBL_ID
            LEFT OUTER JOIN DBS C0 ON B0.DB_ID = C0.DB_ID
            WHERE
                B0.TBL_NAME = 'my_table' AND
                C0.NAME = 'my_schema' AND
                A0.PART_NAME = 'ds=2023-01-01';
            """
        # Normalize whitespace for comparison
        assert " ".join(op.sql.split()) == " ".join(expected_sql.split())

    @mock.patch("airflow.providers.common.sql.sensors.sql.SqlSensor._get_hook")
    def test_poke_table_split(self, mock_get_hook):
        op = MetastorePartitionSensor(
            task_id="test_task", table="other_schema.other_table", partition_name="ds=2023-01-01"
        )

        hook = mock.MagicMock()
        mock_get_hook.return_value = hook
        hook.get_records.return_value = []  # Return empty result

        result = op.poke({})

        assert result is False
        assert op.schema == "other_schema"
        assert op.table == "other_table"

        expected_sql = """
            SELECT 'X'
            FROM PARTITIONS A0
            LEFT OUTER JOIN TBLS B0 ON A0.TBL_ID = B0.TBL_ID
            LEFT OUTER JOIN DBS C0 ON B0.DB_ID = C0.DB_ID
            WHERE
                B0.TBL_NAME = 'other_table' AND
                C0.NAME = 'other_schema' AND
                A0.PART_NAME = 'ds=2023-01-01';
            """
        assert " ".join(op.sql.split()) == " ".join(expected_sql.split())
