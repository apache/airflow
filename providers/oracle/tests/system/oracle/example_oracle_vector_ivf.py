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

"""Exercise an Oracle IVF vector index with FLOAT64 embeddings.

The Dag creates a vector table, ingests documents in batches, and creates an
IVF index with neighbor partitions. It then uses a nested ``$and`` metadata
filter combining ``$in`` and ``$gte`` in a Euclidean-distance search. A
TaskFlow task validates the returned ID, metadata, embedding, and score, which
covers the IVF index path, FLOAT64 binds, and non-trivial JSON filtering.
The final task removes the test documents.
"""

from __future__ import annotations

import datetime
import os
from typing import Any

from airflow.providers.oracle.operators.oracle_vector import (
    OracleAddVectorDocumentsOperator,
    OracleCreateVectorIndexOperator,
    OracleCreateVectorTableOperator,
    OracleDeleteVectorDocumentsOperator,
    OracleVectorSearchOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

TABLE_NAME = "AIRFLOW_VECTOR_IVF_DOCS"
INDEX_NAME = "AIRFLOW_VECTOR_IVF_DOCS_IDX"
ORACLE_CONN_ID = os.environ.get("ORACLE_CONN_ID", "oracle_default")
DOCUMENT_IDS = ["database-vector", "analytics-vector", "search-vector", "other-vector"]


@task
def validate_search_results(results: list[dict[str, Any]]) -> None:
    """Validate that the filtered IVF search returns the nearest matching document."""
    if not results:
        raise ValueError("IVF vector search returned no results")

    nearest = results[0]
    if nearest["id"] != "database-vector":
        raise ValueError(f"Expected database-vector as the nearest result; got {nearest['id']!r}")
    if nearest["metadata"] != {"rank": 5, "topic": "database"}:
        raise ValueError(f"Unexpected metadata for nearest result: {nearest['metadata']!r}")
    if nearest["embedding"] != [1.0, 0.0, 0.0]:
        raise ValueError(f"Unexpected embedding for nearest result: {nearest['embedding']!r}")
    if nearest["distance"] is None or nearest["distance"] < 0:
        raise ValueError(f"Unexpected distance for nearest result: {nearest['distance']!r}")


with DAG(
    dag_id="example_oracle_vector_ivf",
    start_date=datetime.datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"oracle_conn_id": ORACLE_CONN_ID},
    tags=["example", "oracle", "vector"],
) as dag:
    create_table = OracleCreateVectorTableOperator(
        task_id="create_vector_table",
        table_name=TABLE_NAME,
        embedding_dimension=3,
        embedding_format="FLOAT64",
        overwrite=True,
        if_not_exists=False,
    )

    add_documents = OracleAddVectorDocumentsOperator(
        task_id="add_documents",
        table_name=TABLE_NAME,
        embedding_format="FLOAT64",
        batch_size=2,
        documents=[
            {
                "id": "database-vector",
                "text": "Oracle Database supports AI Vector Search.",
                "metadata": {"rank": 5, "topic": "database"},
                "embedding": [1.0, 0.0, 0.0],
            },
            {
                "id": "analytics-vector",
                "text": "Apache Airflow orchestrates analytics pipelines.",
                "metadata": {"rank": 3, "topic": "analytics"},
                "embedding": [0.8, 0.2, 0.0],
            },
            {
                "id": "search-vector",
                "text": "Vector search retrieves semantically similar content.",
                "metadata": {"rank": 1, "topic": "search"},
                "embedding": [0.6, 0.4, 0.0],
            },
            {
                "id": "other-vector",
                "text": "An unrelated document.",
                "metadata": {"rank": 10, "topic": "other"},
                "embedding": [0.0, 0.0, 1.0],
            },
        ],
    )

    # [START howto_operator_oracle_vector_ivf_index]
    create_index = OracleCreateVectorIndexOperator(
        task_id="create_ivf_vector_index",
        table_name=TABLE_NAME,
        index_name=INDEX_NAME,
        index_type="IVF",
        distance="EUCLIDEAN",
        accuracy=100,
        neighbor_partitions=2,
        if_not_exists=True,
    )
    # [END howto_operator_oracle_vector_ivf_index]

    # [START howto_operator_oracle_vector_ivf_search]
    search = OracleVectorSearchOperator(
        task_id="search_documents",
        table_name=TABLE_NAME,
        embedding=[1.0, 0.0, 0.0],
        embedding_format="FLOAT64",
        k=2,
        distance="EUCLIDEAN",
        filter={"$and": [{"topic": {"$in": ["database", "analytics"]}}, {"rank": {"$gte": 3}}]},
        include_score=True,
        include_embedding=True,
    )
    # [END howto_operator_oracle_vector_ivf_search]

    validate_results = validate_search_results(search.output)

    delete_documents = OracleDeleteVectorDocumentsOperator(
        task_id="delete_documents",
        table_name=TABLE_NAME,
        ids=DOCUMENT_IDS,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_table >> add_documents >> create_index >> search >> validate_results
    [add_documents, validate_results] >> delete_documents

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
