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

"""Exercise an Oracle HNSW vector index with FLOAT32 embeddings.

The Dag creates a vector table, ingests sample documents, and creates an HNSW
index. It then performs a metadata-filtered cosine-distance search, logs the
returned scores and embeddings, and removes the test documents.
"""

from __future__ import annotations

import datetime
import logging
import os
from typing import Any

from airflow.providers.oracle.operators.oracle_vector import (
    OracleAddVectorDocumentsOperator,
    OracleCreateVectorIndexOperator,
    OracleCreateVectorTableOperator,
    OracleDeleteVectorDocumentsOperator,
    OracleVectorSearchOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

TABLE_NAME = "AIRFLOW_VECTOR_DOCS"
INDEX_NAME = "AIRFLOW_VECTOR_DOCS_HNSW_IDX"
ORACLE_CONN_ID = os.environ.get("ORACLE_CONN_ID", "oracle_default")


@task
def log_search_results(results: list[dict[str, Any]]) -> None:
    logging.getLogger(__name__).info("Oracle vector search results: %s", results)


with DAG(
    dag_id="example_oracle_vector",
    start_date=datetime.datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"oracle_conn_id": ORACLE_CONN_ID},
    tags=["example", "oracle", "vector"],
) as dag:
    # [START howto_operator_oracle_vector_create_table]
    create_table = OracleCreateVectorTableOperator(
        task_id="create_vector_table",
        table_name=TABLE_NAME,
        embedding_dimension=3,
        overwrite=True,
        if_not_exists=False,
    )
    # [END howto_operator_oracle_vector_create_table]

    # [START howto_operator_oracle_vector_add_documents]
    add_documents = OracleAddVectorDocumentsOperator(
        task_id="add_documents",
        table_name=TABLE_NAME,
        documents=[
            {
                "id": "doc-1",
                "text": "Oracle Database supports AI Vector Search.",
                "metadata": {"source": "example", "topic": "oracle"},
                "embedding": [1.0, 0.0, 0.0],
            },
            {
                "id": "doc-2",
                "text": "Apache Airflow orchestrates data pipelines.",
                "metadata": {"source": "example", "topic": "airflow"},
                "embedding": [0.0, 1.0, 0.0],
            },
            {
                "id": "doc-3",
                "text": "Vector search retrieves semantically similar content.",
                "metadata": {"source": "example", "topic": "search"},
                "embedding": [0.0, 0.0, 1.0],
            },
        ],
    )
    # [END howto_operator_oracle_vector_add_documents]

    # [START howto_operator_oracle_vector_create_index]
    create_index = OracleCreateVectorIndexOperator(
        task_id="create_vector_index",
        table_name=TABLE_NAME,
        index_name=INDEX_NAME,
        index_type="HNSW",
        distance="COSINE",
        accuracy=90,
        neighbors=32,
        ef_construction=200,
        if_not_exists=True,
    )
    # [END howto_operator_oracle_vector_create_index]

    # [START howto_operator_oracle_vector_search]
    search = OracleVectorSearchOperator(
        task_id="search_documents",
        table_name=TABLE_NAME,
        embedding=[1.0, 0.0, 0.0],
        k=2,
        distance="COSINE",
        filter={"source": {"$eq": "example"}},
        include_score=True,
        include_embedding=True,
    )
    # [END howto_operator_oracle_vector_search]

    # [START howto_operator_oracle_vector_delete_documents]
    delete_documents = OracleDeleteVectorDocumentsOperator(
        task_id="delete_documents",
        table_name=TABLE_NAME,
        ids=["doc-1", "doc-2", "doc-3"],
    )
    # [END howto_operator_oracle_vector_delete_documents]

    search_results = log_search_results(search.output)
    create_table >> add_documents >> create_index >> search >> search_results >> delete_documents


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
