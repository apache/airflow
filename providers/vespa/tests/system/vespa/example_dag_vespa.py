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

from datetime import datetime

from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.vespa.operators.vespa_ingest import VespaIngestOperator


@dag(
    dag_id="example_vespa_ingest",
    start_date=datetime(2025, 7, 30),
    schedule="@once",
    catchup=False,
    tags=["example", "vespa"],
)
def vespa_dynamic():
    vespa_conn_id = "vespa_connection"

    @task
    def build_batches():
        return [
            [
                {"id": "doc1", "fields": {"body": "first document"}},
                {"id": "doc2", "fields": {"body": "second document"}},
            ],
            [
                {"id": "doc3", "fields": {"body": "third document"}},
                {"id": "doc4", "fields": {"body": "fourth document"}},
            ],
        ]

    batches = build_batches()

    # [START howto_operator_vespa_ingest]
    send_batches = VespaIngestOperator.partial(vespa_conn_id=vespa_conn_id, task_id="send_batch").expand(
        docs=batches
    )

    update_doc3 = VespaIngestOperator(
        vespa_conn_id=vespa_conn_id,
        task_id="update_doc3",
        docs=[{"id": "doc3", "fields": {"body": "third document - UPDATED"}}],
        operation_type="update",
    )

    delete_doc4 = VespaIngestOperator(
        vespa_conn_id=vespa_conn_id,
        task_id="delete_doc4",
        docs=[{"id": "doc4"}],
        operation_type="delete",
    )
    # [END howto_operator_vespa_ingest]

    @task(task_id="verify_docs_with_hook")
    def verify_docs_with_hook():
        """Use the VespaHook to validate the example documents."""
        from airflow.providers.vespa.hooks.vespa import VespaHook

        hook = VespaHook(conn_id=vespa_conn_id)

        def run_query(yql: str, params: dict | None = None):
            params = params or {}
            res = hook.vespa_app.query(yql=yql, **params)
            hits = res.hits if hasattr(res, "hits") else []
            return {
                "count": len(hits),
                "first": hits[0].get("fields") if hits else None,
            }

        q1 = run_query('select * from sources * where body contains "first document"')
        q2 = run_query('select * from sources * where body contains "second document"')
        q3_updated = run_query('select * from sources * where body contains "UPDATED"')
        q4_deleted = run_query('select * from sources * where body contains "fourth document"')

        assert q1["count"] >= 1, "doc1 not found"
        assert q2["count"] >= 1, "doc2 not found"
        assert q3_updated["count"] >= 1, "updated doc3 not found"
        assert q4_deleted["count"] == 0, "doc4 appears to still be present"

        return {
            "doc1_found": q1["count"],
            "doc2_found": q2["count"],
            "doc3_updated_found": q3_updated["count"],
            "doc4_found": q4_deleted["count"],
        }

    verify_docs_task = verify_docs_with_hook()
    send_batches >> [update_doc3, delete_doc4] >> verify_docs_task


example_dag = vespa_dynamic()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(example_dag)
