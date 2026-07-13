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
"""
Example DAGs: gate a consumer Dag on the data quality score of the asset that triggers it.

Two Dags, wired together only through the ``dq_gated_orders`` asset:

- ``example_dq_require_quality_producer`` checks a table and produces the asset.
  :class:`~airflow.providers.common.dataquality.operators.dq_check.DQCheckOperator` attaches its summary
  (including the quality ``score``) to the asset event automatically because the asset carries
  quality config attached with :func:`~airflow.providers.common.dataquality.assets.asset_quality`.
- ``example_dq_require_quality_consumer`` is scheduled by that asset. Its first task, built by
  :func:`~airflow.providers.common.dataquality.assets.require_quality`, reads the score off the triggering
  asset event and short-circuits -- skipping every downstream task -- when it's missing or
  below ``min_score``. Downstream tasks never see a run triggered by a bad batch of data.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.providers.common.dataquality.assets import asset_quality, require_quality
from airflow.providers.common.dataquality.operators.dq_check import DQCheckOperator
from airflow.providers.common.dataquality.rules import DQRule, RuleSet
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG, Asset, task

CONN_ID = "sqlite_default"
TABLE_NAME = "dq_gated_orders"
RESULTS_PATH = Path("/tmp/airflow_dq_example/results")

os.environ.setdefault("AIRFLOW__COMMON_DATAQUALITY__RESULTS_PATH", f"file://{RESULTS_PATH}")

# [START howto_asset_quality]
gated_orders_ruleset = RuleSet(
    name="gated_orders_quality",
    rules=(
        DQRule(name="order_id_not_null", check="null_count", column="order_id", condition={"equal_to": 0}),
        DQRule(name="amount_min_ge_zero", check="min", column="amount", condition={"geq_to": 0}),
        DQRule(name="row_count_present", check="row_count", condition={"greater_than": 0}),
    ),
)

gated_orders_asset = asset_quality(
    Asset("dq_gated_orders", uri="file:///tmp/airflow_dq_example/gated_orders"),
    ruleset=gated_orders_ruleset,
    conn_id=CONN_ID,
    table=TABLE_NAME,
)
# [END howto_asset_quality]

with DAG(
    dag_id="example_dq_require_quality_producer",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dq"],
) as producer_dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql=[
            f"DROP TABLE IF EXISTS {TABLE_NAME};",
            f"CREATE TABLE {TABLE_NAME} (order_id INTEGER, amount REAL);",
        ],
    )

    insert_orders = SQLExecuteQueryOperator(
        task_id="insert_orders",
        conn_id=CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (order_id, amount) VALUES
            (1, 10.0),
            (2, 25.5),
            (3, 7.25);
        """,
    )

    # Producing check: no explicit outlets needed, DQCheckOperator adds the asset itself
    # because it was passed via asset=.
    check_orders = DQCheckOperator(
        task_id="check_orders",
        asset=gated_orders_asset,
        fail_on="never",
    )

    create_table >> insert_orders >> check_orders

# [START howto_require_quality]
with DAG(
    dag_id="example_dq_require_quality_consumer",
    schedule=gated_orders_asset,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dq"],
) as consumer_dag:
    gate = require_quality(gated_orders_asset, min_score=0.95)

    @task
    def process_orders():
        print("Processing orders -- quality gate passed.")

    gate >> process_orders()
# [END howto_require_quality]
