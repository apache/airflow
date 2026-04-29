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

from airflow.providers.amazon.aws.operators.glue_catalog import (
    GlueCatalogCreateDatabaseOperator,
    GlueCatalogDeleteDatabaseOperator,
)
from airflow.providers.common.compat.sdk import DAG, chain

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import TriggerRule
else:
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "example_glue_catalog"

sys_test_context_task = SystemTestContextBuilder().build()

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    db_name = f"{env_id}_test_db"

    # [START howto_operator_glue_catalog_create_database]
    create_database = GlueCatalogCreateDatabaseOperator(
        task_id="create_database",
        database_name=db_name,
        description="Test database for Glue Catalog",
    )
    # [END howto_operator_glue_catalog_create_database]

    # [START howto_operator_glue_catalog_delete_database]
    delete_database = GlueCatalogDeleteDatabaseOperator(
        task_id="delete_database",
        database_name=db_name,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_glue_catalog_delete_database]

    chain(
        test_context,
        create_database,
        delete_database,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
