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

Route pipeline failures to a fix or a person
============================================

A task failed overnight and the on-call engineer has to read the error, decide whether it
was a blip worth rerunning, something a person has to fix now, or noise to ignore, and
then act. This Dag hands the reading to a model that returns a pick and how sure it is.
Airflow runs only the chosen branch, sends uncertain picks to a human with a deadline, and
demands more confidence to page someone than to rerun a task.

What this demonstrates
----------------------

* :doc:`../operators/llm_branch` -- ``LLMBranchOperator`` picks one downstream task id.
* :doc:`../classifier_models` -- a classifier model, so the gate has a confidence to read.
* :doc:`../approval_gates` -- ``on_uncertain="review"`` routes low-confidence picks to a
  human, with ``approval_timeout`` bounding the wait.

Run it
------

1. Install the provider with the classifier extra:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[typesafe]"

2. Set ``{"model": "typesafe:jev-1.13.0"}`` in the ``pydanticai_default`` extra. The second
   Dag below reads the same kind of connection as ``jev_default``.

3. Trigger the Dag:

   .. code-block:: bash

       airflow dags test example_llm_branch_decision_policy

The ``triage_failure`` log shows the pick and its confidence. Exactly one of ``rerun``,
``page_oncall`` and ``ignore`` runs. Below the confidence bar the run pauses for review
instead (Airflow 3.1 or later); answer from Required Actions in the UI (see the note on
:doc:`index`).

The Dag
-------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_operator_llm_branch_decision_policy]
    :end-before: [END howto_operator_llm_branch_decision_policy]

Classify-then-act variant
-------------------------

When the action depends on the score itself, classify in one task and act in the next
(:doc:`../classifier_models` explains reading the score). It uses the ``jev_default``
connection; run it as ``airflow dags test example_classifier_model_confidence``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_classifier_model.py
    :language: python
    :start-after: [START howto_classifier_model_confidence]
    :end-before: [END howto_classifier_model_confidence]

Adapting it
-----------

* Replace the hard-coded ``prompt`` with the failed task's own error. An
  ``on_failure_callback`` on the production Dag can trigger this one with the exception
  text in ``conf``, and the prompt reads ``{{ dag_run.conf["error"] }}``.
* Make the branch tasks do the work: ``rerun`` clears the failed task instance through
  the REST API, ``page_oncall`` posts to your alerting provider.
* Tune ``min_confidence`` per branch. A wrong page costs more than an extra rerun, so
  ``page_oncall`` demands more certainty.
* To decide retry versus fail inside Airflow's retry loop, see :doc:`../retry_policies`.
