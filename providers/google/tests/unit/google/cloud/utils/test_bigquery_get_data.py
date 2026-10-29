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
from unittest import mock

import pytest
from google.cloud.bigquery.table import Row, RowIterator

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.utils.bigquery_get_data import bigquery_get_data


class TestBigqueryGetData:
    def test_yields_successive_batches(self):
        mock_hook = mock.MagicMock(spec=BigQueryHook)
        mock_logger = mock.MagicMock(spec=logging.Logger)
        rows = [mock.MagicMock(spec=Row) for _ in range(3)]
        for row, value in zip(rows, "ABC"):
            row.values.return_value = (value,)
        mock_hook.list_rows.side_effect = [rows[:2], rows[2:], []]

        results = list(
            bigquery_get_data(
                logger=mock_logger,
                dataset_id="my_dataset",
                table_id="my_table",
                big_query_hook=mock_hook,
                batch_size=2,
                selected_fields=["col1"],
            )
        )

        assert results == [[("A",), ("B",)], [("C",)]]
        assert mock_hook.list_rows.mock_calls == [
            mock.call(
                dataset_id="my_dataset",
                table_id="my_table",
                max_results=2,
                selected_fields=["col1"],
                start_index=start_index,
            )
            for start_index in (0, 2, 4)
        ]
        mock_logger.info.assert_any_call("Job Finished")

    def test_raises_type_error_for_row_iterator(self):
        mock_hook = mock.MagicMock(spec=BigQueryHook)
        mock_hook.list_rows.return_value = mock.MagicMock(spec=RowIterator)

        with pytest.raises(TypeError, match="returns iterator when return_iterator=False"):
            list(
                bigquery_get_data(
                    logger=mock.MagicMock(spec=logging.Logger),
                    dataset_id="my_dataset",
                    table_id="my_table",
                    big_query_hook=mock_hook,
                    batch_size=1,
                    selected_fields=["col1"],
                )
            )
