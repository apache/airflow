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

from airflow.providers.amazon.aws.operators.s3_vectors import (
    S3VectorsCreateIndexOperator,
    S3VectorsCreateVectorBucketOperator,
    S3VectorsDeleteIndexOperator,
    S3VectorsDeleteVectorBucketOperator,
)
from airflow.providers.common.compat.sdk import DAG, chain

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import TriggerRule
else:
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "example_s3_vectors"

sys_test_context_task = SystemTestContextBuilder().build()


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    bucket_name = f"{env_id}-test-vectors"
    index_name = f"{env_id}-test-index"

    # [START howto_operator_s3vectors_create_vector_bucket]
    create_vector_bucket = S3VectorsCreateVectorBucketOperator(
        task_id="create_vector_bucket",
        vector_bucket_name=bucket_name,
    )
    # [END howto_operator_s3vectors_create_vector_bucket]

    # [START howto_operator_s3vectors_create_index]
    create_index = S3VectorsCreateIndexOperator(
        task_id="create_index",
        vector_bucket_name=bucket_name,
        index_name=index_name,
        data_type="float32",
        dimension=128,
        distance_metric="cosine",
    )
    # [END howto_operator_s3vectors_create_index]

    # [START howto_operator_s3vectors_delete_vector_bucket]
    delete_vector_bucket = S3VectorsDeleteVectorBucketOperator(
        task_id="delete_vector_bucket",
        vector_bucket_name=bucket_name,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_s3vectors_delete_vector_bucket]

    # [START howto_operator_s3vectors_delete_index]
    delete_index = S3VectorsDeleteIndexOperator(
        task_id="delete_index",
        vector_bucket_name=bucket_name,
        index_name=index_name,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_s3vectors_delete_index]

    chain(
        test_context,
        create_vector_bucket,
        create_index,
        delete_index,
        delete_vector_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
