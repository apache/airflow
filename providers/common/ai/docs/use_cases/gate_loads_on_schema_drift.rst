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

Block a load when the schema drifts
===================================

A nightly load copies ``customers`` from Postgres into Snowflake. One morning a column was
renamed upstream and the load either failed halfway or, worse, succeeded with nulls. This
Dag compares the two schemas before the load and asks the model which differences would
break it. Airflow branches on the answer: compatible schemas run the load, anything else
notifies the team. The model only reports; no migration runs.

What this demonstrates
----------------------

* :doc:`../operators/llm_schema_compare` -- ``@task.llm_schema_compare`` reads both
  schemas through Airflow connections and returns a structured comparison with a
  ``compatible`` flag.
* :ref:`Branching <apache-airflow:concepts:branching>` with ``@task.branch``.

Run it
------

1. Install the provider with the SQL extra and the providers for your databases:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[openai,sql]" \
           apache-airflow-providers-postgres apache-airflow-providers-snowflake

2. Create database connections ``postgres_source`` and ``snowflake_target`` that both
   contain a ``customers`` table.

3. Trigger the Dag:

   .. code-block:: bash

       airflow dags test example_llm_schema_compare_conditional

The ``check_before_etl`` XCom holds the full comparison. Exactly one of ``run_etl`` and
``notify_team`` runs.

The Dag
-------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_schema_compare.py
    :language: python
    :start-after: [START howto_operator_llm_schema_compare_conditional]
    :end-before: [END howto_operator_llm_schema_compare_conditional]

Adapting it
-----------

* Point ``db_conn_ids`` and ``table_names`` at your own source and target. The operator
  also compares against files on object storage, for example a Parquet landing zone; see
  :doc:`../operators/llm_schema_compare`.
* Replace the ``run_etl`` placeholder with your load task and ``notify_team`` with a
  Slack or email notifier.
* Add an :doc:`../approval_gates` step before ``run_etl`` when a compatible-but-changed
  schema should still get a human's confirmation.
* Set ``schedule`` to match the load and give the Dag the same ``start_date`` so the two
  stay aligned.
