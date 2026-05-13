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
"""Example DAGs demonstrating LLMDataQualityOperator usage."""

from __future__ import annotations

import logging
import re

from airflow.providers.common.ai.operators.llm_data_quality import LLMDataQualityOperator
from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
from airflow.providers.common.ai.toolsets.dataquality.sql import SQLDQToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.ai.utils.dataquality import (
    DQCheckInput,
    exact_check,
    null_pct_check,
    register_validator,
    row_count_check,
)
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.common.sql.config import DataSourceConfig

# ------------------------------------------------------------------
# Module-level custom validator used across multiple DAGs
# ------------------------------------------------------------------

_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# [START howto_operator_llm_dq_email_format_validator]
@register_validator(
    "email_format_check",
    llm_context=(
        "Row-level validator. Run `SELECT email FROM <table>` (no aggregation), "
        "collect all email values into a list, and pass that list to apply_validator. "
        "The validator checks each value against a basic email regex and passes when "
        "the fraction of invalid emails is at or below max_invalid_pct."
    ),
    check_category="validity",
    row_level=True,
)
def email_format_check(*, max_invalid_pct: float = 0.0):
    """Return a per-row predicate used by row-level aggregation in SQLDQToolset."""

    def _check(value: object) -> bool:
        if value in (None, ""):
            return False
        return bool(_EMAIL_PATTERN.match(str(value)))

    return _check


# [END howto_operator_llm_dq_email_format_validator]


# [START howto_operator_llm_dq_postgres_basic]
@dag
def example_llm_dq_postgres_basic():
    """
    Run data-quality checks on PostgreSQL tables using explicit toolsets.

    :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` handles schema
    discovery and SQL execution, while
    :class:`~airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset` provides the
    ``list_validators`` and ``apply_validator`` tools to the LLM agent.

    Connections required:

    - ``pydanticai_default``: Pydantic AI connection.
    - ``postgres_default``: PostgreSQL connection pointing at the seeded database.
    """
    LLMDataQualityOperator(
        task_id="validate_orders_and_customers",
        llm_conn_id="pydanticai_default",
        toolsets=[
            SQLToolset(db_conn_id="postgres_default", allowed_tables=["orders", "customers"]),
            SQLDQToolset(),
        ],
        checks=[
            DQCheckInput(
                name="null_order_id",
                description="Check the percentage of rows in orders where order_id is NULL",
            ),
            DQCheckInput(
                name="duplicate_orders",
                description="Calculate the percentage of duplicate order_id values in orders",
            ),
            DQCheckInput(
                name="null_customer_email",
                description="Check the percentage of rows in customers where email is NULL",
            ),
            DQCheckInput(
                name="min_order_count",
                description="Count the total number of rows in the orders table",
                validator=row_count_check(min_count=1),
            ),
            DQCheckInput(
                name="negative_amount",
                description="Count rows in orders where order_amount is negative or NULL",
                validator=exact_check(expected=0),
            ),
            DQCheckInput(
                name="customer_email_format",
                description="Validate that each email in the customers table matches a valid email format",
                validator=email_format_check(max_invalid_pct=0.01),
            ),
        ],
    )


# [END howto_operator_llm_dq_postgres_basic]

example_llm_dq_postgres_basic()


# [START howto_operator_llm_dq_explicit_toolsets]
@dag
def example_llm_dq_explicit_toolsets():
    """
    Run data-quality checks with explicitly composed toolsets.

    Passing ``toolsets`` directly gives full control over which tables the
    agent can access and which validators are available.  Use this pattern
    when you need fine-grained permissions or want to add custom toolsets.

    Connections required:

    - ``pydanticai_default``: Pydantic AI connection.
    - ``postgres_default``: PostgreSQL connection.
    """
    LLMDataQualityOperator(
        task_id="validate_products",
        llm_conn_id="pydanticai_default",
        toolsets=[
            SQLToolset(db_conn_id="postgres_default", allowed_tables=["products"]),
            SQLDQToolset(),
        ],
        checks=[
            DQCheckInput(
                name="null_sku",
                description="Check the percentage of rows where sku is NULL",
                validator=null_pct_check(max_pct=0.0),
            ),
            DQCheckInput(
                name="negative_price",
                description="Count rows where price is negative or NULL",
                validator=exact_check(expected=0),
            ),
            DQCheckInput(
                name="min_product_count",
                description="Ensure the products table has at least 100 rows",
                validator=row_count_check(min_count=100),
            ),
        ],
    )


# [END howto_operator_llm_dq_explicit_toolsets]

example_llm_dq_explicit_toolsets()


# [START howto_operator_llm_dq_task_decorator]
@dag
def example_llm_dq_task_decorator():
    """
    Combine a ``@task``-decorated step with a ``@task.llm_dq``-decorated DQ task.

    ``notify_start`` is a plain ``@task`` that runs first.
    ``validate_users`` is decorated with ``@task.llm_dq``: the function body
    returns the checks list, and the decorator handles the LLM agent run,
    SQL execution, and metric validation.

    Connections required:

    - ``pydanticai_default``: Pydantic AI connection.
    - ``postgres_default``: PostgreSQL connection for the ``users`` table.
    """

    @task
    def notify_start() -> dict:
        log = logging.getLogger(__name__)
        log.info("Starting DQ validation for the users table.")
        return {"status": "started"}

    @task.llm_dq(
        llm_conn_id="pydanticai_default",
        toolsets=[
            SQLToolset(db_conn_id="postgres_default", allowed_tables=["users"]),
            SQLDQToolset(),
        ],
    )
    def validate_users():
        return [
            DQCheckInput(
                name="null_username",
                description="Check the percentage of rows where username is NULL",
            ),
            DQCheckInput(
                name="null_email",
                description="Check the percentage of rows where email is NULL",
                validator=null_pct_check(max_pct=0.02),
            ),
        ]

    notify_start() >> validate_users()


# [END howto_operator_llm_dq_task_decorator]

example_llm_dq_task_decorator()


# [START howto_operator_llm_dq_require_approval]
@dag
def example_llm_dq_require_approval():
    """
    Gate check execution on human review using the built-in HITL mechanism.

    When ``require_approval=True`` the task runs the LLM planning phase first,
    then defers *before* SQL execution and validator application so the generated
    check plan (names, descriptions, and pre-assigned validators) can be reviewed
    in the Airflow UI.  SQL queries and validators only run after the reviewer
    approves.  If rejected, the task raises
    :class:`~airflow.providers.standard.exceptions.HITLRejectException`.

    Connections required:

    - ``pydanticai_default``: Pydantic AI connection.
    - ``postgres_default``: PostgreSQL connection for the ``orders`` table.
    """
    LLMDataQualityOperator(
        task_id="validate_orders_with_approval",
        llm_conn_id="pydanticai_default",
        toolsets=[
            SQLToolset(db_conn_id="postgres_default", allowed_tables=["orders"]),
            SQLDQToolset(),
        ],
        require_approval=True,
        checks=[
            {"name": "null_order_id", "description": "Check the percentage of rows where order_id is NULL"},
            {
                "name": "negative_price",
                "description": "Check the percentage of rows where order_amount is negative or NULL",
            },
        ],
    )


# [END howto_operator_llm_dq_require_approval]

example_llm_dq_require_approval()


# [START howto_operator_llm_dq_object_storage]
@dag
def example_llm_dq_object_storage():
    """
    Run data-quality checks on a Parquet dataset stored in S3 via Apache DataFusion.

    :class:`~airflow.providers.common.ai.toolsets.datafusion.DataFusionToolset`
    registers the S3 source as a queryable table named ``events`` and gives the
    LLM agent ``list_tables``, ``get_schema``, and ``query`` tools backed by
    Apache DataFusion.  :class:`~airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset`
    adds the ``list_validators`` and ``apply_validator`` tools so the agent can
    apply built-in validators to each check metric.

    The ``region`` key in the ``aws_default`` connection **Extra** field must
    match the bucket's AWS region to avoid redirect errors, e.g.::

        {"region": "eu-central-1"}

    Connections required:

    - ``pydanticai_default``: Pydantic AI connection.
    - ``aws_default``: AWS connection with read access to the S3 bucket.
    """
    datasource_config = DataSourceConfig(
        conn_id="aws_default",
        table_name="events",
        uri="s3://my-bucket/events/",
        format="parquet",
    )

    LLMDataQualityOperator(
        task_id="validate_events",
        llm_conn_id="pydanticai_default",
        toolsets=[
            DataFusionToolset(datasource_configs=[datasource_config]),
            SQLDQToolset(),
        ],
        checks=[
            DQCheckInput(
                name="null_event_id",
                description="Check the percentage of rows where event_id is NULL",
                validator=null_pct_check(max_pct=0.0),
            ),
            DQCheckInput(
                name="negative_amount",
                description="Count rows where amount is negative or NULL",
                validator=exact_check(expected=0),
            ),
            DQCheckInput(
                name="min_event_count",
                description="Count the total number of rows in the events table",
                validator=row_count_check(min_count=1),
            ),
        ],
    )


# [END howto_operator_llm_dq_object_storage]

example_llm_dq_object_storage()
