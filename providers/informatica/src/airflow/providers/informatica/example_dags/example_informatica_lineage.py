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
Example DAG for Informatica EDC lineage.

This DAG demonstrates automatic and manual lineage reporting to Informatica
EDC using the ``apache-airflow-providers-informatica`` provider.

Prerequisites
-------------
1. Start the local dev stack::

       cd providers/informatica/dev
       docker-compose up -d

2. Create the Airflow connections (UI → Admin → Connections):

   **informatica_edc_default**
     - Connection Type : HTTP
     - Host            : localhost  (or ``informatica_sim`` inside Breeze)
     - Port            : 8082
     - Schema          : http

   **postgres_lineage**
     - Connection Type : Postgres
     - Host            : localhost  (or ``informatica-lineage-postgres`` inside Breeze)
     - Port            : 55433      (host mapping, 5432 inside Breeze network)
     - Schema          : lineage_demo
     - Login           : airflow
     - Password        : airflow

3. Enable auto-lineage in ``airflow.cfg`` (or via env var)::

       [informatica]
       auto_lineage_enabled = True

What this DAG demonstrates
---------------------------
* **Auto-lineage (task: build_order_summary)**
  A multi-join SELECT → INSERT.  The provider parses the SQL with ``sqlglot``,
  extracts ``orders``, ``customers``, ``products`` as inlets and
  ``order_summary`` as the outlet, then reports the relationship to EDC.

* **Manual lineage (task: compute_customer_ltv)**
  Inlets / outlets are set explicitly on the task via ``task.inlets`` and
  ``task.outlets``.  Auto-detection is skipped when manual lineage is present.

* **Lineage disabled per-task (task: truncate_staging)**
  ``disable_informatica_lineage(truncate_staging)`` is called after task
  creation, so the provider skips EDC reporting entirely for this task.

* **Lineage disabled via operator class (task: log_run_metadata)**
  The operator's fully-qualified class name is added to
  ``[informatica] disabled_for_operators`` in the example config block
  (commented out here; uncomment to test).

* **Strict pre-execution validation (task: compute_customer_ltv)**
  ``pre_execute=validate_informatica_lineage`` is set on the operator so
  that the task fails **before** ``execute()`` if any inlet/outlet URI
  cannot be resolved in the EDC catalog.  Without ``pre_execute``, the
  listener logs a warning but does not block execution.

* **Generic transfer with explicit target table (task: build_customer_segment_snapshot_generic)**
    A complex ``CTE``-based ``SELECT`` is executed by ``GenericTransfer`` and loaded
    into ``destination_table='customer_segment_snapshot'``.

Inspecting results
------------------
After a successful DAG run, check the simulator's in-memory lineage store::

    curl http://localhost:8082/lineage
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.informatica.lineage import disable_informatica_lineage
from airflow.providers.informatica.lineage.validation import validate_informatica_lineage
from airflow.sdk import Asset

task_logger = logging.getLogger("airflow.task")
task_logger.setLevel(logging.DEBUG)

# ---------------------------------------------------------------------------
# Airflow connection IDs used by all tasks
# ---------------------------------------------------------------------------

_PG_CONN = "postgres_lineage"

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="example_informatica_lineage",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "informatica", "lineage"],
    doc_md=__doc__,
) as dag:
    # ------------------------------------------------------------------
    # Task 1 — AUTO-LINEAGE
    # sqlglot detects:
    #   inlets  : orders, customers, products
    #   outlet  : order_summary
    # ------------------------------------------------------------------
    build_order_summary = SQLExecuteQueryOperator(
        task_id="build_order_summary",
        conn_id=_PG_CONN,
        sql="""
            INSERT INTO order_summary (
                order_id,
                customer_email,
                product_name,
                category,
                quantity,
                total_amount,
                order_date,
                country
            )
            SELECT
                o.order_id,
                c.email          AS customer_email,
                p.product_name,
                p.category,
                o.quantity,
                o.quantity * o.unit_price AS total_amount,
                o.order_date,
                c.country
            FROM orders o
            JOIN customers c ON c.customer_id = o.customer_id
            JOIN products  p ON p.product_id  = o.product_id
            WHERE o.status = 'completed'
            ON CONFLICT (order_id) DO UPDATE
              SET processed_at = NOW()
        """,
    )

    # ------------------------------------------------------------------
    # Task 2 — MANUAL LINEAGE + STRICT PRE-EXECUTE VALIDATION
    # Inlets/outlets are declared explicitly; auto-detection is skipped.
    # pre_execute=validate_informatica_lineage fails the task *before*
    # execute() if any URI cannot be resolved in the EDC catalog.
    # ------------------------------------------------------------------
    compute_customer_ltv = SQLExecuteQueryOperator(
        task_id="compute_customer_ltv",
        conn_id=_PG_CONN,
        pre_execute=validate_informatica_lineage,
        sql="""
            INSERT INTO customer_ltv (
                customer_id,
                email,
                full_name,
                country,
                total_orders,
                total_spent,
                avg_order_value,
                first_order_date,
                last_order_date
            )
            SELECT
                c.customer_id,
                c.email,
                c.first_name || ' ' || c.last_name AS full_name,
                c.country,
                COUNT(o.order_id)                   AS total_orders,
                SUM(o.quantity * o.unit_price)       AS total_spent,
                AVG(o.quantity * o.unit_price)       AS avg_order_value,
                MIN(o.order_date)                    AS first_order_date,
                MAX(o.order_date)                    AS last_order_date
            FROM customers c
            LEFT JOIN orders o ON o.customer_id = c.customer_id
            GROUP BY c.customer_id, c.email, c.first_name, c.last_name, c.country
            ON CONFLICT (customer_id) DO UPDATE
              SET total_orders     = EXCLUDED.total_orders,
                  total_spent      = EXCLUDED.total_spent,
                  avg_order_value  = EXCLUDED.avg_order_value,
                  first_order_date = EXCLUDED.first_order_date,
                  last_order_date  = EXCLUDED.last_order_date,
                  calculated_at    = NOW()
        """,
        # Declare inlets/outlets manually — EDC URI format: edc://object/<identifier>
        inlets=[
            Asset("TEST_PSTGRS://mydb/public/customers"),
            Asset("TEST_PSTGRS://mydb/public/orders"),
        ],
        outlets=[
            Asset("TEST_PSTGRS://mydb/public/customer_ltv"),
        ],
    )

    # ------------------------------------------------------------------
    # Task 3 — GENERIC TRANSFER (COMPLEX CTE + EXPLICIT TARGET TABLE)
    # Auto-lineage reads source tables from sql and uses destination_table
    # as explicit outlet target for GenericTransfer.
    # ------------------------------------------------------------------
    build_customer_segment_snapshot_generic = GenericTransfer(
        task_id="build_customer_segment_snapshot_generic",
        source_conn_id=_PG_CONN,
        destination_conn_id=_PG_CONN,
        destination_table="customer_segment_snapshot",
        preoperator="""
            CREATE TABLE IF NOT EXISTS customer_segment_snapshot (
                customer_id BIGINT,
                email TEXT,
                country TEXT,
                total_orders BIGINT,
                total_revenue NUMERIC(14, 2),
                avg_order_value NUMERIC(14, 2),
                last_order_date DATE,
                top_category TEXT,
                segment TEXT
            );
            TRUNCATE TABLE customer_segment_snapshot;
        """,
        sql="""
            WITH order_base AS (
                SELECT
                    o.customer_id,
                    o.order_id,
                    o.order_date,
                    o.quantity,
                    o.unit_price,
                    (o.quantity * o.unit_price) AS gross_amount,
                    p.category,
                    p.product_name
                FROM orders o
                JOIN products p ON p.product_id = o.product_id
                WHERE o.status = 'completed'
            ),
            customer_rollup AS (
                SELECT
                    ob.customer_id,
                    COUNT(DISTINCT ob.order_id) AS total_orders,
                    SUM(ob.gross_amount) AS total_revenue,
                    AVG(ob.gross_amount) AS avg_order_value,
                    MAX(ob.order_date) AS last_order_date
                FROM order_base ob
                GROUP BY ob.customer_id
            ),
            category_mix AS (
                SELECT
                    ob.customer_id,
                    ob.category,
                    SUM(ob.gross_amount) AS category_revenue,
                    ROW_NUMBER() OVER (
                        PARTITION BY ob.customer_id
                        ORDER BY SUM(ob.gross_amount) DESC, ob.category
                    ) AS category_rank
                FROM order_base ob
                GROUP BY ob.customer_id, ob.category
            ),
            top_category AS (
                SELECT
                    cm.customer_id,
                    cm.category AS top_category
                FROM category_mix cm
                WHERE cm.category_rank = 1
            ),
            segmented AS (
                SELECT
                    c.customer_id,
                    c.email,
                    c.country,
                    cr.total_orders,
                    cr.total_revenue,
                    cr.avg_order_value,
                    cr.last_order_date,
                    tc.top_category,
                    NTILE(4) OVER (ORDER BY cr.total_revenue DESC NULLS LAST) AS revenue_quartile
                FROM customer_rollup cr
                JOIN customers c ON c.customer_id = cr.customer_id
                LEFT JOIN top_category tc ON tc.customer_id = cr.customer_id
            )
            SELECT
                customer_id,
                email,
                country,
                total_orders,
                total_revenue,
                avg_order_value,
                last_order_date,
                top_category,
                CASE
                    WHEN revenue_quartile = 1 THEN 'platinum'
                    WHEN revenue_quartile = 2 THEN 'gold'
                    WHEN revenue_quartile = 3 THEN 'silver'
                    ELSE 'bronze'
                END AS segment
            FROM segmented
            WHERE total_orders >= 1
        """,
    )

    # ------------------------------------------------------------------
    # Task 4 — LINEAGE DISABLED PER-TASK
    # This task touches staging data; we don't want it reported to EDC.
    # disable_informatica_lineage() sets a special Param that the listener
    # checks at runtime.
    # ------------------------------------------------------------------
    truncate_staging = SQLExecuteQueryOperator(
        task_id="truncate_staging",
        conn_id=_PG_CONN,
        sql="TRUNCATE TABLE order_summary",
    )
    # Disable EDC lineage for this task only
    disable_informatica_lineage(truncate_staging)

    # ------------------------------------------------------------------
    # Task 5 — SIMPLE READ-ONLY QUERY (auto-lineage: inlets only)
    # No INSERT/CREATE/MERGE → no outlet will be detected automatically.
    # ------------------------------------------------------------------
    verify_counts = SQLExecuteQueryOperator(
        task_id="verify_counts",
        conn_id=_PG_CONN,
        sql="""
            SELECT
                (SELECT COUNT(*) FROM order_summary)  AS summary_rows,
                (SELECT COUNT(*) FROM customer_ltv)   AS ltv_rows,
                (SELECT COUNT(*) FROM customer_segment_snapshot) AS segment_rows
        """,
    )

    # ------------------------------------------------------------------
    # Pipeline order
    # ------------------------------------------------------------------
    (
        truncate_staging
        >> build_order_summary
        >> compute_customer_ltv
        >> build_customer_segment_snapshot_generic
        >> verify_counts
    )
