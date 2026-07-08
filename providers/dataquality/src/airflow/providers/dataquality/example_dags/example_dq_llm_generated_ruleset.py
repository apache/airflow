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
Example DAG: generate a ``RuleSet`` with an LLM, guided by the ``dataquality-rule-authoring`` skill.

Requires the optional ``apache-airflow-providers-common-ai[skills]`` package and a configured
LLM connection (``llm_conn_id``); this Dag is not registered when common.ai isn't installed.

An LLM is asked to propose data quality rules for a table's columns. Its ``system_prompt``
points it at the ``dataquality-rule-authoring`` skill shipped with this provider (via
``AgentSkillsToolset``), so it knows the exact ``RuleSet``/``DQRule`` schema, the built-in check
catalog, and the ``Condition`` grammar, instead of guessing at field or check names.
``output_type=RuleSet`` makes pydantic-ai validate -- and self-correct -- the model's output
against the real ``RuleSet`` model before the task completes, so a malformed rule never reaches
``DQCheckOperator`` in the first place.

The generated ``RuleSet`` is then returned by ``@task.dq_check`` at runtime, since the real
ruleset doesn't exist until the LLM task runs.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.dataquality.rules import RuleSet
from airflow.sdk import DAG, task

try:
    from airflow.providers.common.ai.toolsets.skills import AgentSkillsToolset
except ImportError:
    AgentSkillsToolset = None  # type: ignore[assignment,misc]

DAG_ID = "example_dq_llm_generated_ruleset"
CONN_ID = "sqlite_default"
TABLE_NAME = "dq_llm_orders"
RESULTS_PATH = Path("/tmp/airflow_dq_example/results")
# The skill ships next to this Dag's package, not next to this file -- resolve relative to
# __file__ so the path holds regardless of the Dag processor's working directory.
SKILLS_DIR = Path(__file__).parent.parent / "skills" / "dataquality-rule-authoring"

os.environ.setdefault("AIRFLOW__DQ__RESULTS_PATH", f"file://{RESULTS_PATH}")

if AgentSkillsToolset is not None:
    with DAG(
        dag_id=DAG_ID,
        schedule=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example", "dq", "ai"],
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
                    region TEXT,
                    created_at TEXT
                );
                """,
            ],
        )

        insert_orders = SQLExecuteQueryOperator(
            task_id="insert_orders",
            conn_id=CONN_ID,
            sql=f"""
            INSERT INTO {TABLE_NAME} (order_id, customer_id, amount, region, created_at) VALUES
                (1, 101, 10.0, 'US', '2026-07-01'),
                (2, 102, 25.5, 'EU', '2026-07-02'),
                (3, NULL, 7.25, 'US', '2026-07-03');
            """,
        )

        # [START howto_task_dq_generate_ruleset_with_llm]
        @task.llm(
            llm_conn_id="pydanticai_default",
            system_prompt=(
                "You are a data quality engineer. Before answering, consult the "
                "dataquality-rule-authoring skill for the exact RuleSet/DQRule schema, the built-in "
                "check catalog, and the Condition grammar -- do not invent field or check names."
            ),
            output_type=RuleSet,
            agent_params={"toolsets": [AgentSkillsToolset(sources=[str(SKILLS_DIR)])]},
        )
        def generate_ruleset():
            return (
                "Generate a RuleSet named 'orders_llm_quality' for a table with these columns: "
                "order_id (integer, primary key), customer_id (integer, nullable foreign key), "
                "amount (float, must be non-negative), region (short string, low-cardinality), "
                "created_at (date). Include at least a not-null check on order_id, a uniqueness "
                "check on order_id, and a row-count check."
            )

        # [END howto_task_dq_generate_ruleset_with_llm]

        # [START howto_decorator_dq_check_llm_runtime_ruleset]
        @task.dq_check(conn_id=CONN_ID, table=TABLE_NAME, fail_on="never")
        def check_with_llm_ruleset(ruleset: RuleSet):
            return ruleset

        # [END howto_decorator_dq_check_llm_runtime_ruleset]

        generated_ruleset = generate_ruleset()
        checked = check_with_llm_ruleset(generated_ruleset)

        create_table >> insert_orders >> generated_ruleset >> checked
