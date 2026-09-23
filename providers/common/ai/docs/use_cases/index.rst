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

.. _howto/use_cases:

What you can build
==================

Each page starts from a job a data team already has, shows the Dag that does it, and names
what Airflow adds over a script: a schedule, a retryable task per item, an approval gate, or
a fan-out sized at runtime. Every Dag ships with the provider and runs against your
connections.

Start with :doc:`triage_support_tickets`. It needs nothing but an LLM connection and shows
the shape most of the others build on: structured output plus dynamic task mapping.

Every Dag needs the provider installed with the extra for your model vendor and a
``pydanticai`` connection named ``pydanticai_default``; :doc:`../quickstart` covers both.
Each page's "Run it" lists only what that Dag adds.

.. list-table::
   :header-rows: 1
   :widths: 28 36 36

   * - Scenario
     - What the model does
     - What Airflow does
   * - :doc:`triage_support_tickets`
     - Reads each ticket and returns priority, category, summary and next action as a typed record
     - One retryable task per ticket, results in XCom, one argument away from a schedule
   * - :doc:`route_pipeline_failures`
     - Picks rerun, page or ignore from the error text, with a confidence score
     - Runs only the chosen branch, sends low-confidence picks to a human
   * - :doc:`gate_loads_on_schema_drift`
     - Compares source and target schemas and reports what would break a load
     - Branches to the load or to a notification, no migration ever runs
   * - :doc:`explain_revenue_anomaly`
     - Queries the warehouse and does the pivots in a sandbox to explain a revenue move
     - Injects the date, holds the credential, destroys the sandbox when the run ends
   * - :doc:`monthly_report_from_a_csv`
     - Turns a fixed question into SQL over a CSV
     - Downloads the file monthly, records schema changes before generating SQL, emails the result
   * - :doc:`compare_10k_filings`
     - Splits a comparison question per company and writes the report
     - Weekly indexing Dag, on-demand analysis Dag, per-company fan-out, review at both ends
   * - :doc:`ask_questions_over_pdfs`
     - Answers a question from retrieved excerpts, citing them
     - Weekly indexing Dag, query Dag triggered with a ``question`` param
   * - :doc:`weekly_status_report`
     - Assesses progress per proposal, then checks its own report against the evidence
     - Mapped evidence gathering, deterministic correction step, human review
   * - :doc:`classify_reviews_in_bulk`
     - Labels sentiment for every review in one batch job
     - Waits up to 24 hours without holding a worker, lands results on object storage
   * - :doc:`research_agent_with_review`
     - Decides which tools to call to answer a research question
     - Human edits the question first, separate formatting step, approval before delivery

More ideas
----------

The same operators cover many other jobs. These do not have an example Dag yet, but each
can be built from the patterns shown on the pages above.

* **Explain a failure in the alert.** A task with ``trigger_rule="one_failed"`` reads the
  failed task's log, ``@task.llm`` returns a ``Literal`` root cause and a two-sentence
  explanation, and the notifier posts that instead of a stack trace.
* **Data-quality triage.** Feed null rates, freshness and duplicate counts from your checks
  task to ``@task.llm`` with a ``Finding`` list as ``output_type``, then
  ``LLMBranchOperator`` to page, file a ticket, or ignore.
* **Daily incident digest.** Fetch alerts for the data interval, one ``@task.llm`` summary
  per service with ``.expand()``, one synthesis call, ``ApprovalOperator`` before it posts.
* **Release notes from merged pull requests.** Weekly ``HttpOperator`` fetch,
  ``@task.llm_batch`` to classify each PR at batch prices, one call to draft the notes, a
  ``HITLEntryOperator`` for the editor's pass.
* **Invoices into a table.** ``ObjectStoragePath`` lists new files, ``@task.llm_file_analysis``
  extracts a typed row from each with ``.expand()``, a SQL operator inserts them, and the
  Dag emits an Asset so downstream reporting runs when the rows land.
* **Rewrite a failing query.** On a SQL task's failure, hand the query and the database
  error to ``@task.llm_sql`` with the schema context, and put the rewrite in front of a
  reviewer before it runs.
* **Tag and route incoming files.** ``@task.llm_file_analysis`` on each new object decides
  its type and sensitivity, ``@task.branch`` moves it to the right bucket.
* **Catalog descriptions.** Nightly, for every table that changed, ``@task.llm`` writes a
  column-level description from the schema and a sample, and a task upserts it into the
  catalog.

:doc:`../examples` lists the same Dags by operator.

.. note::

    Dags with ``HITLEntryOperator`` or ``ApprovalOperator`` pause under ``airflow dags test``
    until someone answers from Required Actions in the UI of an api-server on the same
    metadata database. ``airflow standalone`` gives you one.

.. toctree::
    :titlesonly:
    :hidden:

    Triage support tickets <triage_support_tickets>
    Route pipeline failures <route_pipeline_failures>
    Block a load on schema drift <gate_loads_on_schema_drift>
    Explain a revenue anomaly <explain_revenue_anomaly>
    Monthly report from a CSV <monthly_report_from_a_csv>
    Compare 10-K filings <compare_10k_filings>
    Ask questions over PDFs <ask_questions_over_pdfs>
    Weekly status report <weekly_status_report>
    Classify reviews in bulk <classify_reviews_in_bulk>
    Research agent with review <research_agent_with_review>
