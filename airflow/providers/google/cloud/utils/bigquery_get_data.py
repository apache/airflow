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

from collections.abc import Iterator
from logging import Logger

from google.cloud.bigquery.table import Row

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def bigquery_get_data(
    logger: Logger,
    dataset_id: str,
    table_id: str,
    big_query_hook: BigQueryHook,
    batch_size: int,
    selected_fields: list[str] | str | None,
) -> Iterator:
    logger.info("Fetching Data from:")
    logger.info("Dataset: %s ; Table: %s", dataset_id, table_id)

    i = 0
    while True:
        rows: list[Row] = big_query_hook.list_rows(
            dataset_id=dataset_id,
            table_id=table_id,
            max_results=batch_size,
            selected_fields=selected_fields,
            start_index=i * batch_size,
        )

        if len(rows) == 0:
            logger.info("Job Finished")
            return

        logger.info("Total Extracted rows: %s", len(rows) + i * batch_size)

        yield [row.values() for row in rows]

        i += 1
