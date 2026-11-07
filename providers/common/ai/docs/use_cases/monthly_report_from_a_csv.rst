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

Monthly report from a survey CSV
================================

A published CSV, a stakeholder who wants the same question answered from it every month,
and nobody who wants to hand-write the SQL or learn from the report that a column was
renamed. This Dag downloads the file, checks its schema against a reference, has the model
write the SQL, runs it with Apache DataFusion, and emails the rows. Airflow supplies the
monthly schedule and a record of every schema change.

The example uses the `Airflow community survey <https://airflow.apache.org/survey/>`__
CSV, which is public and needs no credentials.

What this demonstrates
----------------------

* :doc:`../operators/llm_sql` -- ``LLMSQLQueryOperator`` turns a question into SQL
  against a described schema, without executing it.
* :doc:`../operators/llm_schema_compare` -- ``LLMSchemaCompareOperator`` records how the
  downloaded file differs from a reference before any SQL is generated. It reports; it does
  not block (see Adapting it).
* ``AnalyticsOperator`` from the ``common.sql`` provider -- runs the generated SQL over a
  local file with DataFusion, no database needed.
* ``HttpOperator`` and ``SmtpHook`` from the ``http`` and ``smtp`` providers do the
  download and delivery.

Run it
------

1. Install the provider with the SQL extra, plus the HTTP and SMTP providers:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[openai,sql]" \
           "apache-airflow-providers-common-sql[datafusion]" \
           apache-airflow-providers-http apache-airflow-providers-smtp

2. Create an HTTP connection named ``airflow_website`` with host
   ``https://airflow.apache.org`` and no auth.

3. Optionally set ``SMTP_CONN_ID`` and ``NOTIFY_EMAIL`` in the environment. Without
   them the result goes to the task log.

4. Trigger the Dag:

   .. code-block:: bash

       airflow dags test example_llm_survey_scheduled

The ``check_schema`` XCom holds the comparison, ``generate_sql`` holds the query the model
wrote, and ``send_result`` logs or mails the rows.

The Dag
-------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_analysis.py
    :language: python
    :start-after: [START example_llm_survey_scheduled]
    :end-before: [END example_llm_survey_scheduled]

Variants
--------

``example_llm_survey_interactive`` in the same file takes ad hoc questions: a
``HITLEntryOperator`` edits the question, an ``ApprovalOperator`` reviews the rows. No
download, no schedule; the CSV is assumed in place.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_survey_analysis.py
    :language: python
    :start-after: [START example_llm_survey_interactive]
    :end-before: [END example_llm_survey_interactive]

When one question is not enough, ``example_llm_survey_agentic`` splits a research question
into sub-questions, maps SQL generation and execution over them, and synthesizes the
results behind an approval gate.

Adapting it
-----------

* Make the schema check block: add a ``@task.branch`` on ``compatible`` between
  ``check_schema`` and ``generate_sql``, as :doc:`gate_loads_on_schema_drift` does.
* Point ``SURVEY_CSV_ENDPOINT`` and the HTTP connection at your own published file, and
  replace ``SURVEY_SCHEMA`` with a description of its columns; that description is what
  the model writes SQL against.
* Change ``SCHEDULED_PROMPT`` to the question the report answers.
* Swap the local-file ``DataSourceConfig`` for a warehouse connection when the data
  lives in a database rather than a file; ``LLMSQLQueryOperator`` accepts either.
