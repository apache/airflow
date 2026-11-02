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
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from airflow.providers.apache.hbase.hooks.hbase_strategy import PooledThrift2Strategy


class TestPooledThrift2StrategyBatching:
    """Test that batch_size controls chunk size"""

    def _make_strategy(self, pool_size=8):
        mock_pool = MagicMock()
        mock_pool.size = pool_size

        mock_client = MagicMock()

        @contextmanager
        def fake_connection():
            yield mock_client

        mock_pool.connection = fake_connection
        strategy = PooledThrift2Strategy(pool=mock_pool, logger=logging.getLogger("test"))
        return strategy, mock_client

    @patch("airflow.providers.apache.hbase.hooks.hbase_strategy.BATCH_DELAY", 0)
    def test_batch_put_rows_respects_batch_size(self):
        """batch_put_rows must chunk by batch_size"""

        strategy, mock_client = self._make_strategy()
        rows = [{"row_key": f"row_{i}", "cf:col": f"val_{i}"} for i in range(1000)]

        strategy.batch_put_rows("test_table", rows, batch_size=200, max_workers=4)

        calls = mock_client.put_multiple.call_args_list
        assert len(calls) == 5  # 1000 / 200 = 5 chunks
        for call in calls:
            _, puts = call[0]  # (table_name, puts)
            assert len(puts) <= 200

    @patch("airflow.providers.apache.hbase.hooks.hbase_strategy.BATCH_DELAY", 0)
    def test_batch_delete_rows_respects_batch_size(self):
        """batch_delete_rows must chunk by batch_size"""

        strategy, mock_client = self._make_strategy()
        row_keys = [f"row_{i}" for i in range(1000)]

        strategy.batch_delete_rows("test_table", row_keys, batch_size=200, max_workers=4)

        calls = mock_client.delete_multiple.call_args_list
        assert len(calls) == 5  # 1000 / 200 = 5 chunks
        for call in calls:
            _, deletes = call[0]  # (table_name, deletes)
            assert len(deletes) <= 200
