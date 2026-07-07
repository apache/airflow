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
Example DAG for the Data Quality provider's ``DQCheckOperator``.

Runs against the ``sqlite_default`` connection (Airflow's default local backend, no extra
infrastructure required). Results are persisted through the ``[dq] results_path`` config
option, seeded here via ``AIRFLOW__DQ__RESULTS_PATH`` so the history can be inspected
afterwards under ``/tmp/airflow_dq_example/results`` -- a real deployment would instead set
``results_path`` in ``airflow.cfg`` (or its env var) once, for every check to share.

Trigger the Dag repeatedly with different ``dq_scenario`` values to create useful
persisted data quality history:

- ``pass`` inserts clean data.
- ``negative`` inserts a negative amount, failing ``amount_non_negative``.
- ``duplicate_order`` inserts a duplicate ``order_id``, failing uniqueness.
- ``nulls`` inserts null ``order_id``/``customer_id`` values and a high discount null ratio.
- ``outlier`` inserts amounts above the allowed maximum.
- ``small_table`` inserts too few rows for volume checks.
- ``zero_quantity`` inserts an invalid quantity minimum.
- ``empty`` leaves the table empty.
- ``mixed`` combines several bad values to show multiple failures in one run.

Demonstrates
three equivalent ways to run the same ruleset:

- Plain ``DQCheckOperator`` with an explicit ``table``/``conn_id``.
- ``asset_quality()`` attaching the ruleset to an ``Asset``, then ``DQCheckOperator(asset=...)``
  picking up ``table``/``conn_id``/``ruleset`` from it and adding the asset to the task's
  outlets automatically.
- A second asset-producing ``DQCheckOperator`` with stricter rules, so the Browse > Data Quality
  page shows multiple producers for the same asset with both pass and fail outcomes.
- The ``@task.dq_check`` TaskFlow decorator.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.dq.assets import asset_quality
from airflow.providers.dq.operators.dq_check import DQCheckOperator
from airflow.providers.dq.rules import DQRule, RuleSet, Severity
from airflow.sdk import DAG, Asset, task

DAG_ID = "example_dq_check"
CONN_ID = "sqlite_default"
TABLE_NAME = "dq_example_orders"
RESULTS_PATH = Path("/tmp/airflow_dq_example/results")

# DQCheckOperator has no per-operator results-store override; every check in a deployment
# shares the one store configured under [dq] results_path. setdefault() so a real deployment's
# own config is never overridden.
os.environ.setdefault("AIRFLOW__DQ__RESULTS_PATH", f"file://{RESULTS_PATH}")

# [START howto_operator_dq_check_ruleset]
orders_ruleset = RuleSet(
    name="orders_quality",
    rules=(
        DQRule(
            name="order_id_not_null",
            check="null_count",
            column="order_id",
            condition={"equal_to": 0},
        ),
        DQRule(
            name="order_id_unique",
            check="unique_violations",
            column="order_id",
            condition={"equal_to": 0},
        ),
        DQRule(
            name="customer_id_not_null",
            check="null_count",
            column="customer_id",
            condition={"equal_to": 0},
            severity=Severity.WARN,
        ),
        DQRule(
            name="discount_null_ratio",
            check="null_ratio",
            column="discount",
            condition={"leq_to": 0.25},
            severity=Severity.WARN,
        ),
        DQRule(
            name="region_distinct_count",
            check="distinct_count",
            column="region",
            condition={"geq_to": 2},
            severity=Severity.WARN,
        ),
        DQRule(
            name="amount_min_ge_zero",
            check="min",
            column="amount",
            condition={"geq_to": 0},
        ),
        DQRule(
            name="amount_max_le_100",
            check="max",
            column="amount",
            condition={"leq_to": 100},
        ),
        DQRule(
            name="amount_mean_between_5_and_60",
            check="mean",
            column="amount",
            condition={"geq_to": 5, "leq_to": 60},
        ),
        DQRule(
            name="quantity_min_ge_one",
            check="min",
            column="quantity",
            condition={"geq_to": 1},
        ),
        DQRule(
            name="quantity_max_le_ten",
            check="max",
            column="quantity",
            condition={"leq_to": 10},
        ),
        DQRule(
            name="amount_non_negative",
            check="custom_sql",
            # {table} is substituted by the SQL engine at check time, not an f-string placeholder.
            sql="SELECT COUNT(*) FROM {table} WHERE amount < 0",
            condition={"equal_to": 0},
        ),
        DQRule(
            name="high_value_order_count",
            check="custom_sql",
            sql="SELECT COUNT(*) FROM {table} WHERE amount > 100",
            condition={"equal_to": 0},
            severity=Severity.WARN,
        ),
        DQRule(
            name="row_count_present",
            check="row_count",
            condition={"greater_than": 0},
            severity=Severity.WARN,
        ),
        DQRule(
            name="row_count_at_least_three",
            check="row_count",
            condition={"geq_to": 3},
        ),
    ),
)
# [END howto_operator_dq_check_ruleset]

# [START howto_operator_dq_check_asset]
orders_asset = asset_quality(
    Asset("dq_example_orders", uri="file:///tmp/airflow_dq_example/orders"),
    ruleset=orders_ruleset,
    conn_id=CONN_ID,
    table=TABLE_NAME,
)
# [END howto_operator_dq_check_asset]

strict_orders_ruleset = RuleSet(
    name="orders_strict_quality",
    rules=(
        DQRule(
            name="strict_order_id_not_null",
            check="null_count",
            column="order_id",
            condition={"equal_to": 0},
        ),
        DQRule(
            name="strict_amount_max_le_20",
            check="max",
            column="amount",
            condition={"leq_to": 20},
        ),
        DQRule(
            name="strict_row_count_equal_two",
            check="row_count",
            condition={"equal_to": 2},
        ),
        DQRule(
            name="strict_region_distinct_count",
            check="distinct_count",
            column="region",
            condition={"geq_to": 4},
            severity=Severity.WARN,
        ),
    ),
)

strict_orders_asset = asset_quality(
    Asset("dq_example_orders", uri="file:///tmp/airflow_dq_example/orders"),
    ruleset=strict_orders_ruleset,
    conn_id=CONN_ID,
    table=TABLE_NAME,
)


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dq"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql=[
            f"DROP TABLE IF EXISTS {TABLE_NAME};",
            f"""
            CREATE TABLE {TABLE_NAME} (
                order_id INTEGER,
                customer_id INTEGER,
                amount REAL,
                discount REAL,
                quantity INTEGER,
                region TEXT
            );
            """,
        ],
    )

    clear_table = SQLExecuteQueryOperator(
        task_id="clear_table",
        conn_id=CONN_ID,
        sql=f"DELETE FROM {TABLE_NAME};",
    )

    insert_orders = SQLExecuteQueryOperator(
        task_id="insert_orders",
        conn_id=CONN_ID,
        sql=f"""
        {{% set scenario = dag_run.conf.get("dq_scenario", "pass") if dag_run else "pass" %}}
        {{% if scenario == "negative" %}}
        INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, discount, quantity, region) VALUES
            (1, 101, 10.0, 0.00, 1, 'US'),
            (2, 102, -25.5, 0.10, 2, 'EU'),
            (3, 103, 7.25, NULL, 3, 'US'),
            (4, 104, 55.0, 0.20, 4, 'APAC');
        {{% elif scenario == "duplicate_order" %}}
        INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, discount, quantity, region) VALUES
            (1, 101, 10.0, 0.00, 1, 'US'),
            (1, 102, 25.5, 0.10, 2, 'EU'),
            (3, 103, 7.25, NULL, 3, 'US'),
            (4, 104, 55.0, 0.20, 4, 'APAC');
        {{% elif scenario == "nulls" %}}
        INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, discount, quantity, region) VALUES
            (1, 101, 10.0, NULL, 1, 'US'),
            (NULL, NULL, 25.5, NULL, 2, 'EU'),
            (3, 103, 7.25, NULL, 3, 'US'),
            (4, 104, 55.0, 0.20, 4, 'APAC');
        {{% elif scenario == "outlier" %}}
        INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, discount, quantity, region) VALUES
            (1, 101, 10.0, 0.00, 1, 'US'),
            (2, 102, 120.0, 0.10, 2, 'EU'),
            (3, 103, 150.0, NULL, 3, 'US'),
            (4, 104, 55.0, 0.20, 4, 'APAC');
        {{% elif scenario == "small_table" %}}
        INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, discount, quantity, region) VALUES
            (1, 101, 10.0, 0.00, 1, 'US');
        {{% elif scenario == "zero_quantity" %}}
        INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, discount, quantity, region) VALUES
            (1, 101, 10.0, 0.00, 0, 'US'),
            (2, 102, 25.5, 0.10, 2, 'EU'),
            (3, 103, 7.25, NULL, 3, 'US'),
            (4, 104, 55.0, 0.20, 4, 'APAC');
        {{% elif scenario == "mixed" %}}
        INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, discount, quantity, region) VALUES
            (1, 101, 10.0, NULL, 0, 'US'),
            (1, NULL, -25.5, NULL, 2, 'US'),
            (NULL, 103, 150.0, NULL, 11, 'US'),
            (4, 104, 55.0, 0.20, 4, 'US');
        {{% elif scenario == "empty" %}}
        INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, discount, quantity, region)
            SELECT NULL, NULL, NULL, NULL, NULL, NULL WHERE 0;
        {{% else %}}
        INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, discount, quantity, region) VALUES
            (1, 101, 10.0, 0.00, 1, 'US'),
            (2, 102, 25.5, 0.10, 2, 'EU'),
            (3, 103, 7.25, NULL, 3, 'US'),
            (4, 104, 55.0, 0.20, 4, 'APAC');
        {{% endif %}}
        """,
    )

    # [START howto_operator_dq_check]
    check_orders = DQCheckOperator(
        task_id="check_orders",
        conn_id=CONN_ID,
        table=TABLE_NAME,
        ruleset=orders_ruleset,
        fail_on="never",
    )
    # [END howto_operator_dq_check]

    check_orders_via_asset = DQCheckOperator(
        task_id="check_orders_via_asset",
        asset=orders_asset,
        fail_on="never",
    )

    check_orders_strict_via_asset = DQCheckOperator(
        task_id="check_orders_strict_via_asset",
        asset=strict_orders_asset,
        fail_on="never",
    )

    # [START howto_decorator_dq_check]
    @task.dq_check(
        conn_id=CONN_ID,
        table=TABLE_NAME,
        ruleset=orders_ruleset,
        fail_on="never",
    )
    def check_orders_decorated():
        return None

    # [END howto_decorator_dq_check]

    (
        create_table
        >> clear_table
        >> insert_orders
        >> [check_orders, check_orders_via_asset, check_orders_strict_via_asset, check_orders_decorated()]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
