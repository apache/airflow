 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Explain a revenue anomaly
=========================

Daily revenue moved more than ten percent against its trailing average and someone has
to find out which region and channel explain it before the morning stand-up. This Dag
gives an agent two tools: read-only queries against two warehouse tables, capped at 500
rows, and a pandas sandbox for the pivots. It returns a typed finding with the suspected
cause and a confidence. Airflow injects the run date, keeps the warehouse credential where
the model never sees it, and destroys the sandbox when the run ends.

What this demonstrates
----------------------

* :doc:`../operators/agent` -- ``AgentOperator`` runs a multi-turn agent as one task,
  with ``output_type`` for a typed result.
* :doc:`../toolsets/sql` -- ``SQLToolset`` exposes named query operations; the model sees
  rows, never the connection.
* :doc:`../sandbox/index` -- ``SandboxToolset`` runs everything the model writes off the
  worker.
* Templating -- ``{{ ds }}`` in the prompt ties the question to the Dag run's date.

Run it
------

1. Install the provider with the SQL and Modal extras and authenticate with Modal:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[openai,sql,modal]"
       modal setup

2. Create a database connection named ``warehouse`` that has
   ``analytics.daily_revenue`` and ``analytics.orders`` tables.

3. Trigger the Dag for a date:

   .. code-block:: bash

       airflow dags test example_sandbox_agent_investigation 2026-03-01

The ``investigate`` task log shows every SQL call and every script the agent ran in the
sandbox, and its XCom holds the ``Findings`` record.

The Dag
-------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_sandbox_toolset.py
    :language: python
    :start-after: [START howto_sandbox_agent_investigation]
    :end-before: [END howto_sandbox_agent_investigation]

Adapting it
-----------

* Trigger it from a data-quality check that fires when the move exceeds your threshold, so
  the agent runs only on days that need explaining.
* Swap ``ModalSandboxBackend`` for ``SbxSandboxBackend`` to run locally; the same file has
  that variant (see :doc:`../sandbox/backends`).
* Add an :doc:`../approval_gates` step before the finding reaches the finance channel.
* Widen ``allowed_tables`` only as far as the question needs. The limit is what makes the
  agent safe to run unattended.
