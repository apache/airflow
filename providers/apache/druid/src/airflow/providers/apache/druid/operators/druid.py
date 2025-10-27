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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.druid.hooks.druid import DruidHook, IngestionType
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class DruidOperator(BaseOperator):
    """
    Allows to submit a task directly to druid.

    :param json_index_file: The filepath to the druid index specification
    :param druid_ingest_conn_id: The connection id of the Druid overlord which
        accepts index jobs
    :param timeout: The interval (in seconds) between polling the Druid job for the status
        of the ingestion job. Must be greater than or equal to 1
    :param max_ingestion_time: The maximum ingestion time before assuming the job failed
    :param ingestion_type: The ingestion type of the job. Could be IngestionType.Batch or IngestionType.MSQ
    :param verify_ssl: Whether to use SSL encryption to submit indexing job. If set to False then checks
                       connection information for path to a CA bundle to use. Defaults to True
    """

    template_fields: Sequence[str] = ("json_index_file",)
    template_ext: Sequence[str] = (".json",)
    template_fields_renderers = {"json_index_file": "json"}

    def __init__(
        self,
        *,
        json_index_file: str,
        druid_ingest_conn_id: str = "druid_ingest_default",
        timeout: int = 1,
        max_ingestion_time: int | None = None,
        ingestion_type: IngestionType = IngestionType.BATCH,
        verify_ssl: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.json_index_file = json_index_file
        self.conn_id = druid_ingest_conn_id
        self.timeout = timeout
        self.max_ingestion_time = max_ingestion_time
        self.ingestion_type = ingestion_type
        self.verify_ssl = verify_ssl

    def execute(self, context: Context) -> None:
        hook = DruidHook(
            druid_ingest_conn_id=self.conn_id,
            timeout=self.timeout,
            max_ingestion_time=self.max_ingestion_time,
            verify_ssl=self.verify_ssl,
        )
        self.log.info("Submitting %s", self.json_index_file)
        hook.submit_indexing_job(self.json_index_file, self.ingestion_type)
