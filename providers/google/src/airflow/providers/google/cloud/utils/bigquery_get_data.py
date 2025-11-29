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

import itertools
from typing import TYPE_CHECKING

from google.cloud.bigquery.table import Row, RowIterator

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from airflow.sdk.types import Logger


def bigquery_get_data(
    logger: Logger,
    dataset_id: str | None,
    table_id: str | None,
    big_query_hook: BigQueryHook,
    batch_size: int,
    selected_fields: list[str] | str | None,
) -> Iterator:
    logger.info("Fetching Data from:")
    logger.info("Dataset: %s ; Table: %s", dataset_id, table_id)

    for start_index in itertools.count(step=batch_size):
        rows: list[Row] | RowIterator = big_query_hook.list_rows(
            dataset_id=dataset_id,
            table_id=table_id,
            max_results=batch_size,
            selected_fields=selected_fields,
            start_index=start_index,
        )

        if isinstance(rows, RowIterator):
            raise TypeError("BigQueryHook.list_rows() returns iterator when return_iterator=False (default)")

        if len(rows) == 0:
            logger.info("Job Finished")
            return

        logger.info("Total Extracted rows: %s", len(rows) + start_index)

        yield [row.values() for row in rows]
