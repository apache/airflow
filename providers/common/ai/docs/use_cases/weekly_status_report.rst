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

Weekly status report with a hallucination check
===============================================

A program manager wants a weekly status on a set of proposals: promised, landed, still
open. The evidence is spread across a wiki and a repository, and people act on the report,
so it has to be right. This Dag gathers the evidence, has the model assess each proposal,
synthesizes a report, then has a second model call judge it against the evidence and a
plain-Python step apply only the flagged corrections. A person reviews the result.

Airflow maps gathering and assessment per proposal, keeps every intermediate in XCom, and
makes the correction step deterministic so the model cannot rewrite the report while
fixing it.

The example tracks Airflow Improvement Proposals against Confluence and GitHub, both
public, and solves the job a second way with one autonomous agent for comparison.

What this demonstrates
----------------------

* :doc:`../operators/llm` -- mapped ``LLMOperator`` calls with structured output for the
  per-proposal analysis, one bounded by ``UsageLimits`` for the synthesis, and one whose
  only job is validation.
* :doc:`../structured_output` -- ``ValidationResult`` lists each claim with a verdict, so
  the correction step has something exact to act on.
* :doc:`apache-airflow-providers-standard:operators/hitl` -- ``ApprovalOperator`` before the
  report goes out.
* :doc:`../toolsets/skills` -- the agent variant loads a ``SKILL.md`` bundle that tells it
  how to assess progress.

Run it
------

1. Install the provider. The agent variant also needs the skills extra:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[openai,skills]"

2. Optionally set ``GITHUB_TOKEN`` in the environment. Without it the Dag paces itself
   to the unauthenticated rate limit and takes longer.

3. Trigger either Dag. Both take an ``aip_numbers`` param:

   .. code-block:: bash

       airflow dags test example_aip_progress_tracker
       airflow dags test example_aip_progress_tracker_skills

The ``validate_report`` XCom shows the disputed claims and ``apply_validation`` shows what
changed. The run pauses at ``review_report``; answer from Required Actions in the UI (see the
note on :doc:`index`).

The Dag
-------

The validation output type and the step that applies it:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py
    :language: python
    :start-after: [START aip_tracker_validation_output]
    :end-before: [END aip_tracker_validation_output]

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py
    :language: python
    :start-after: [START aip_tracker_validation]
    :end-before: [END aip_tracker_validation]

The mapped analysis that feeds it:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py
    :language: python
    :start-after: [START aip_tracker_dtm_analysis]
    :end-before: [END aip_tracker_dtm_analysis]

The full Dag, evidence gathering included, is ``example_aip_progress_tracker`` in
`example_aip_progress_tracker.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py>`__.
It is long because the evidence gathering is real.

The agent variant
-----------------

The same job as one operator call. The agent reads a skill that explains how to assess a
proposal, gets the wiki and repository as tool functions, and decides its own order of
work:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_aip_progress_tracker.py
    :language: python
    :start-after: [START aip_tracker_skills_operator]
    :end-before: [END aip_tracker_skills_operator]

Use the pipeline when every step must be auditable. Use the agent when a fixed task graph
would only re-encode what the model can work out from the skill.

Adapting it
-----------

* Replace the Confluence and GitHub fetchers with your own sources: a project tracker, a
  design-doc folder, a deployment log. Everything from ``analyze_aip`` down is
  source-agnostic.
* Keep the validation step even if you drop the rest. It is cheap and catches the failures
  that make people stop trusting generated reports.
* Put it on ``schedule="@weekly"`` and send the approved report to a channel from a task
  after ``review_report``.
