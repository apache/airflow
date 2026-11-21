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

"""Exercise upserting documents into an Oracle vector table.

The Dag creates a table with custom column names and inserts an initial set of
documents. It then upserts one of them with ``mutate_on_duplicate=True`` and
searches the table using the custom columns. A TaskFlow task verifies that the
search returns the updated text and metadata. The final task removes the test
documents.
"""

from __future__ import annotations

import datetime
import os
from typing import Any

from airflow.providers.oracle.operators.oracle_vector import (
    OracleAddVectorDocumentsOperator,
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

TABLE_NAME = "AIRFLOW_VECTOR_UPSERT_DOCS"
ORACLE_CONN_ID = os.environ.get("ORACLE_CONN_ID", "oracle_default")
ID_COLUMN = "document_id"
TEXT_COLUMN = "content"
METADATA_COLUMN = "attributes"
EMBEDDING_COLUMN = "vector_data"
DOCUMENT_IDS = ["guide", "reference"]


@task
def validate_updated_document(results: list[dict[str, Any]]) -> None:
    """Validate that vector search returns the document updated by the upsert."""
    if len(results) != 1:
        raise ValueError(f"Expected one updated document; got {len(results)}")

    result = results[0]
    if result["id"] != "guide":
        raise ValueError(f"Expected guide as the updated document; got {result['id']!r}")
    if result["text"] != "Updated guide to Oracle AI Vector Search.":
        raise ValueError(f"Unexpected updated document text: {result['text']!r}")
    if result["metadata"] != {"revision": 2, "source": "updated"}:
        raise ValueError(f"Unexpected updated document metadata: {result['metadata']!r}")
    if result["distance"] is None or result["distance"] < 0:
        raise ValueError(f"Unexpected updated document distance: {result['distance']!r}")


with DAG(
    dag_id="example_oracle_vector_upsert",
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
        id_column=ID_COLUMN,
        text_column=TEXT_COLUMN,
        metadata_column=METADATA_COLUMN,
        embedding_column=EMBEDDING_COLUMN,
        overwrite=True,
        if_not_exists=False,
    )

    add_documents = OracleAddVectorDocumentsOperator(
        task_id="add_documents",
        table_name=TABLE_NAME,
        id_column=ID_COLUMN,
        text_column=TEXT_COLUMN,
        metadata_column=METADATA_COLUMN,
        embedding_column=EMBEDDING_COLUMN,
        documents=[
            {
                "id": "guide",
                "text": "Original guide to Oracle AI Vector Search.",
                "metadata": {"revision": 1, "source": "original"},
                "embedding": [0.0, 1.0, 0.0],
            },
            {
                "id": "reference",
                "text": "Reference material for vector search.",
                "metadata": {"revision": 1, "source": "reference"},
                "embedding": [1.0, 0.0, 0.0],
            },
        ],
    )

    # [START howto_operator_oracle_vector_upsert]
    upsert_document = OracleAddVectorDocumentsOperator(
        task_id="upsert_document",
        table_name=TABLE_NAME,
        id_column=ID_COLUMN,
        text_column=TEXT_COLUMN,
        metadata_column=METADATA_COLUMN,
        embedding_column=EMBEDDING_COLUMN,
        mutate_on_duplicate=True,
        documents=[
            {
                "id": "guide",
                "text": "Updated guide to Oracle AI Vector Search.",
                "metadata": {"revision": 2, "source": "updated"},
                "embedding": [0.0, 1.0, 0.0],
            }
        ],
    )
    # [END howto_operator_oracle_vector_upsert]

    search = OracleVectorSearchOperator(
        task_id="search_updated_document",
        table_name=TABLE_NAME,
        embedding=[0.0, 1.0, 0.0],
        k=1,
        distance="EUCLIDEAN",
        filter={"source": {"$eq": "updated"}},
        id_column=ID_COLUMN,
        text_column=TEXT_COLUMN,
        metadata_column=METADATA_COLUMN,
        embedding_column=EMBEDDING_COLUMN,
        include_score=True,
    )

    validate_document = validate_updated_document(search.output)

    delete_documents = OracleDeleteVectorDocumentsOperator(
        task_id="delete_documents",
        table_name=TABLE_NAME,
        ids=DOCUMENT_IDS,
        id_column=ID_COLUMN,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_table >> add_documents >> upsert_document >> search >> validate_document
    [add_documents, upsert_document, validate_document] >> delete_documents

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
