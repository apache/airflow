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

Triage support tickets
======================

A queue of free-text support tickets arrives every hour and someone has to read each one,
decide how urgent it is, and hand it to the right team. This Dag has the model do the
reading and produce a typed record per ticket: priority, category, a one-line summary and
a suggested next action. Airflow runs one task per ticket, so a bad ticket retries alone,
every result is in XCom, and one ``schedule`` argument makes it hourly.

What this demonstrates
----------------------

* :doc:`../operators/llm` -- ``@task.llm`` returns a prompt string; the operator makes
  the call and pushes the result.
* :doc:`../structured_output` -- ``output_type=TicketAnalysis`` gives the downstream task
  a Pydantic instance, not a string to parse.
* :doc:`apache-airflow:authoring-and-scheduling/dynamic-task-mapping` -- ``.expand()``
  fans one model call out per ticket.

Run it
------

1. Install the provider with the extra for your vendor:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[openai]"

2. Trigger the Dag:

   .. code-block:: bash

       airflow dags test example_llm_analysis_pipeline

The ``store_results`` log has one ``[PRIORITY] category: summary`` line per ticket; each
``analyze_ticket`` map index holds a ``TicketAnalysis`` XCom.

The Dag
-------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_analysis_pipeline.py
    :language: python
    :start-after: [START howto_decorator_llm_pipeline]
    :end-before: [END howto_decorator_llm_pipeline]

Adapting it
-----------

* Replace the list in ``get_support_tickets`` with a query against your ticketing system,
  for example an ``SQLExecuteQueryOperator`` or an ``HttpOperator`` upstream.
* Set ``schedule="@hourly"`` on the ``@dag`` and use ``{{ data_interval_start }}`` in
  the query so each run picks up only new tickets.
* Make ``priority`` and ``category`` ``Literal`` types on ``TicketAnalysis`` so the
  model cannot invent a label. :doc:`route_pipeline_failures` shows how to branch on the
  result.
