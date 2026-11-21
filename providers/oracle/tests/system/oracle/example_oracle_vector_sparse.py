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

"""Exercise Oracle sparse-vector ingestion and search.

The Dag creates a sparse vector table, inserts native ``oracledb.SparseVector``
values, searches with another native sparse vector, validates the XCom-safe
result, and removes the test documents.
"""

from __future__ import annotations

import datetime
import os
from array import array
from typing import Any

import oracledb

from airflow.providers.oracle.hooks.oracle_vector import OracleVectorDocument, OracleVectorHook
from airflow.providers.oracle.operators.oracle_vector import OracleCreateVectorTableOperator
from airflow.providers.oracle.vector import vector_to_result
from airflow.utils.trigger_rule import TriggerRule

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

TABLE_NAME = "AIRFLOW_VECTOR_SPARSE_DOCS"
ORACLE_CONN_ID = os.environ.get("ORACLE_CONN_ID", "oracle_default")
DOCUMENT_IDS = ["sparse-database", "sparse-analytics"]


# [START howto_oracle_vector_sparse]
@task
def insert_and_search() -> list[dict[str, Any]]:
    """
    Insert sparse vectors and search with a sparse query vector.
    """
    hook = OracleVectorHook(oracle_conn_id=ORACLE_CONN_ID)
    hook.add_documents(
        table_name=TABLE_NAME,
        documents=[
            OracleVectorDocument(
                id="sparse-database",
                text="Oracle Database supports AI Vector Search.",
                metadata={"topic": "database"},
                embedding=oracledb.SparseVector(5, [0, 3], array("f", [1.0, 0.5])),
            ),
            OracleVectorDocument(
                id="sparse-analytics",
                text="Apache Airflow orchestrates analytics pipelines.",
                metadata={"topic": "analytics"},
                embedding=oracledb.SparseVector(5, [1, 4], array("f", [1.0, 0.5])),
            ),
        ],
    )
    results = hook.similarity_search_by_vector(
        table_name=TABLE_NAME,
        embedding=oracledb.SparseVector(5, [0, 3], array("f", [1.0, 0.5])),
        k=1,
        include_score=True,
        include_embedding=True,
    )
    return [
        {
            "id": result.id,
            "metadata": result.metadata,
            "distance": result.distance,
            "embedding": vector_to_result(result.embedding) if result.embedding is not None else None,
        }
        for result in results
    ]


# [END howto_oracle_vector_sparse]


@task
def validate_search_results(results: list[dict[str, Any]]) -> None:
    """
    Validate the sparse-vector search result and XCom serialization.
    """
    if not results:
        raise ValueError("Sparse vector search returned no results")

    nearest = results[0]
    if nearest["id"] != "sparse-database":
        raise ValueError(f"Expected sparse-database as the nearest result; got {nearest['id']!r}")
    if nearest["metadata"] != {"topic": "database"}:
        raise ValueError(f"Unexpected metadata for nearest result: {nearest['metadata']!r}")
    if nearest["embedding"] != {"num_dimensions": 5, "indices": [0, 3], "values": [1.0, 0.5]}:
        raise ValueError(f"Unexpected sparse embedding: {nearest['embedding']!r}")
    if nearest["distance"] is None or nearest["distance"] < 0:
        raise ValueError(f"Unexpected distance for nearest result: {nearest['distance']!r}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_documents() -> None:
    """
    Remove documents created by the system test.
    """
    OracleVectorHook(oracle_conn_id=ORACLE_CONN_ID).delete_documents(table_name=TABLE_NAME, ids=DOCUMENT_IDS)


with DAG(
    dag_id="example_oracle_vector_sparse",
    start_date=datetime.datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"oracle_conn_id": ORACLE_CONN_ID},
    tags=["example", "oracle", "vector", "sparse"],
) as dag:
    create_table = OracleCreateVectorTableOperator(
        task_id="create_sparse_vector_table",
        table_name=TABLE_NAME,
        embedding_dimension=5,
        sparse=True,
        overwrite=True,
        if_not_exists=False,
    )

    search_results = insert_and_search()
    validate_results = validate_search_results(search_results)
    cleanup = delete_documents()

    create_table >> search_results >> validate_results >> cleanup

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
