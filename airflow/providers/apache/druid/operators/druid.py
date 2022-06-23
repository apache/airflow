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

from typing import TYPE_CHECKING, Any, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.apache.druid.hooks.druid import DruidHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DruidOperator(BaseOperator):
    """
    Allows to submit a task directly to druid

    :param json_index_file: The filepath to the druid index specification
    :param druid_ingest_conn_id: The connection id of the Druid overlord which
        accepts index jobs
    :param timeout: The interval (in seconds) between polling the Druid job for the status
        of the ingestion job. Must be greater than or equal to 1
    :param max_ingestion_time: The maximum ingestion time before assuming the job failed
    """

    template_fields: Sequence[str] = ('json_index_file',)
    template_ext: Sequence[str] = ('.json',)
    template_fields_renderers = {'json_index_file': 'json'}

    def __init__(
        self,
        *,
        json_index_file: str,
        druid_ingest_conn_id: str = 'druid_ingest_default',
        timeout: int = 1,
        max_ingestion_time: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.json_index_file = json_index_file
        self.conn_id = druid_ingest_conn_id
        self.timeout = timeout
        self.max_ingestion_time = max_ingestion_time

    def execute(self, context: "Context") -> None:
        hook = DruidHook(
            druid_ingest_conn_id=self.conn_id,
            timeout=self.timeout,
            max_ingestion_time=self.max_ingestion_time,
        )
        self.log.info("Submitting %s", self.json_index_file)
        hook.submit_indexing_job(self.json_index_file)
