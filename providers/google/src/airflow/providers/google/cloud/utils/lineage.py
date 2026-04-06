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

from google.cloud.bigquery import CopyJob, ExtractJob, LoadJob, QueryJob

from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.common.sql.hooks.lineage import send_sql_hook_lineage

log = logging.getLogger(__name__)


def _add_bq_table_to_lineage(collector, context, table_ref, *, is_input: bool):
    method = collector.add_input_asset if is_input else collector.add_output_asset
    method(
        context=context,
        scheme="bigquery",
        asset_kwargs={
            "project_id": table_ref.project,
            "dataset_id": table_ref.dataset_id,
            "table_id": table_ref.table_id,
        },
    )


def _add_gcs_uris_to_lineage(collector, context, uris, *, is_input: bool):
    method = collector.add_input_asset if is_input else collector.add_output_asset
    for uri in uris or []:
        method(context=context, uri=uri)


def send_hook_lineage_for_bq_job(context, job):
    """
    Send hook-level lineage for a BigQuery job to the lineage collector.

    Handles all four BigQuery job types:
    - QUERY: delegates to send_sql_hook_lineage for SQL parsing
    - LOAD: source URIs (GCS) as inputs, destination table as output
    - COPY: source tables as inputs, destination table as output
    - EXTRACT: source table as input, destination URIs (GCS) as outputs

    :param context: The hook instance used as lineage context.
    :param job: A BigQuery job object (QueryJob, LoadJob, CopyJob, or ExtractJob).
    """
    collector = get_hook_lineage_collector()

    if isinstance(job, QueryJob):
        log.debug("Sending Hook Level Lineage for Query job.")
        send_sql_hook_lineage(
            context=context,
            sql=job.query,
            job_id=job.job_id,
            default_db=job.default_dataset.project if job.default_dataset else None,
            default_schema=job.default_dataset.dataset_id if job.default_dataset else None,
        )
        return

    try:
        if isinstance(job, LoadJob):
            log.debug("Sending Hook Level Lineage for Load job.")
            _add_gcs_uris_to_lineage(collector, context, job.source_uris, is_input=True)
            if job.destination:
                _add_bq_table_to_lineage(collector, context, job.destination, is_input=False)
        elif isinstance(job, CopyJob):
            log.debug("Sending Hook Level Lineage for Copy job.")
            for source_table in job.sources or []:
                _add_bq_table_to_lineage(collector, context, source_table, is_input=True)
            if job.destination:
                _add_bq_table_to_lineage(collector, context, job.destination, is_input=False)
        elif isinstance(job, ExtractJob):
            log.debug("Sending Hook Level Lineage for Extract job.")
            if job.source:
                _add_bq_table_to_lineage(collector, context, job.source, is_input=True)
            _add_gcs_uris_to_lineage(collector, context, job.destination_uris, is_input=False)
    except Exception as e:
        log.warning("Sending BQ job hook level lineage failed: %s", f"{e.__class__.__name__}: {str(e)}")
        log.debug("Exception details:", exc_info=True)
