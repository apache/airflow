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
"""Example DAGs demonstrating bound methods, functools.partial, and callable objects as agent tools.

These patterns are supported natively — no BaseToolset subclass needed.
"""

from __future__ import annotations

import functools

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.compat.sdk import dag, task

# ---------------------------------------------------------------------------
# 1. Bound method: methods on a service class passed directly as tools
# ---------------------------------------------------------------------------


# [START howto_agent_bound_method_tools]
@dag(schedule=None, tags=["example"])
def example_agent_bound_method_tools():
    """Pass bound methods of a service class directly as agent tools."""

    class InventoryService:
        """Thin wrapper around an inventory data source."""

        def __init__(self, warehouse_id: str) -> None:
            self._warehouse_id = warehouse_id

        def get_stock_level(self, product_id: str) -> int:
            """Return the current stock count for a product in this warehouse."""
            # Replace with a real DB/API call in production.
            mock_stock = {"SKU-001": 42, "SKU-002": 0, "SKU-003": 17}
            return mock_stock.get(product_id, -1)

        def list_low_stock(self, threshold: int = 10) -> list[str]:
            """Return product IDs whose stock is at or below *threshold*."""
            mock_stock = {"SKU-001": 42, "SKU-002": 0, "SKU-003": 17}
            return [pid for pid, qty in mock_stock.items() if qty <= threshold]

    service = InventoryService(warehouse_id="WH-EU-01")

    AgentOperator(
        task_id="inventory_analyst",
        prompt="Which products are running low and what are their exact stock levels?",
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are a warehouse inventory assistant. "
            "Use the tools to identify low-stock products and report their quantities."
        ),
        # Bound methods are passed directly — __name__ and __doc__ are picked up automatically.
        toolsets=[service.get_stock_level, service.list_low_stock],
    )


# [END howto_agent_bound_method_tools]

example_agent_bound_method_tools()


# ---------------------------------------------------------------------------
# 2. functools.partial: pre-configure a generic function for a specific context
# ---------------------------------------------------------------------------


# [START howto_agent_partial_tools]
@dag(schedule=None, tags=["example"])
def example_agent_partial_tools():
    """Pre-configure generic functions with functools.partial before passing as tools."""

    def fetch_metric(environment: str, metric_name: str) -> float:
        """Fetch a named metric value from the given environment."""
        # Replace with a real metrics API call in production.
        mock = {
            ("prod", "error_rate"): 0.012,
            ("prod", "p99_latency_ms"): 145.0,
            ("prod", "requests_per_second"): 3200.0,
        }
        return mock.get((environment, metric_name), 0.0)

    def list_available_metrics(environment: str) -> list[str]:
        """List the metric names available in the given environment."""
        return ["error_rate", "p99_latency_ms", "requests_per_second"]

    # Pre-bind the environment so the agent only needs to supply metric_name.
    prod_fetch_metric = functools.partial(fetch_metric, "prod")
    prod_list_metrics = functools.partial(list_available_metrics, "prod")

    AgentOperator(
        task_id="sre_analyst",
        prompt="Is the production service healthy? Check error rate and latency.",
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are an SRE assistant. "
            "Use the tools to inspect production metrics and summarise service health."
        ),
        # functools.partial — tool name is taken from the underlying function (__func__.__name__).
        toolsets=[prod_fetch_metric, prod_list_metrics],
    )


# [END howto_agent_partial_tools]

example_agent_partial_tools()


# ---------------------------------------------------------------------------
# 3. Callable object: a class with __call__ encapsulating shared state
# ---------------------------------------------------------------------------


# [START howto_agent_callable_object_tools]
@dag(schedule=None, tags=["example"])
def example_agent_callable_object_tools():
    """Pass a callable object (class with __call__) directly as an agent tool."""

    class CustomerLookup:
        """Look up customer details from a shared in-memory store."""

        def __init__(self, customer_data: dict) -> None:
            self._data = customer_data

        def __call__(self, customer_id: str) -> dict:
            """Return name, tier, and lifetime value for the given customer ID."""
            return self._data.get(customer_id, {"error": f"Customer {customer_id!r} not found"})

    lookup = CustomerLookup(
        customer_data={
            "C-001": {"name": "Acme Corp", "tier": "enterprise", "ltv_usd": 85000},
            "C-002": {"name": "Globex Ltd", "tier": "pro", "ltv_usd": 12000},
            "C-003": {"name": "Initech", "tier": "starter", "ltv_usd": 900},
        }
    )

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are a customer success assistant. "
            "Use the CustomerLookup tool to retrieve customer details and answer questions. "
            "Always call CustomerLookup with the customer_id from the question before answering. "
            "Do not guess customer attributes without a tool lookup."
        ),
        # Callable object — tool name defaults to the class name (CustomerLookup).
        toolsets=[lookup],
    )
    def analyse(question: str) -> str:
        return question

    analyse("Call CustomerLookup for customer C-001 and report that customer's tier and lifetime value.")


# [END howto_agent_callable_object_tools]

example_agent_callable_object_tools()


# ---------------------------------------------------------------------------
# 4. Mixed: combine all three callable patterns in one agent
# ---------------------------------------------------------------------------


# [START howto_agent_mixed_callable_tools]
@dag(schedule=None, tags=["example"])
def example_agent_mixed_callable_tools():
    """Mix bound methods, functools.partial, and callable objects in a single agent."""

    # --- bound method ---
    class OrderService:
        def get_order(self, order_id: str) -> dict:
            """Fetch order details by order ID."""
            mock = {
                "ORD-1": {"status": "shipped", "items": 3, "total_usd": 299.0},
                "ORD-2": {"status": "pending", "items": 1, "total_usd": 49.0},
            }
            return mock.get(order_id, {"error": "not found"})

    order_service = OrderService()

    # --- functools.partial ---
    def send_notification(channel: str, message: str) -> str:
        """Send *message* to a notification *channel* and return a confirmation."""
        # Replace with a real Slack/email call in production.
        return f"Sent to {channel!r}: {message}"

    notify_ops = functools.partial(send_notification, "ops-alerts")

    # --- callable object ---
    class ExchangeRate:
        def __call__(self, currency: str) -> float:
            """Return the current USD exchange rate for the given currency code."""
            rates = {"EUR": 1.08, "GBP": 1.27, "JPY": 0.0067}
            return rates.get(currency.upper(), 1.0)

    exchange_rate = ExchangeRate()

    AgentOperator(
        task_id="order_ops_agent",
        prompt=(
            "Check orders ORD-1 and ORD-2. Convert ORD-1's total to EUR and send a summary to ops-alerts."
        ),
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are an order operations assistant. "
            "Use the available tools to look up orders, convert currencies, and send notifications."
        ),
        toolsets=[
            order_service.get_order,  # bound method
            notify_ops,  # functools.partial
            exchange_rate,  # callable object
        ],
    )


# [END howto_agent_mixed_callable_tools]

example_agent_mixed_callable_tools()
