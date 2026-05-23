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

.. NOTE TO CONTRIBUTORS:
    Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
    and you want to add an explanation to the users on how they are supposed to deal with them.
    The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-common-ai``

Changelog
---------

0.4.0
.....

Features
~~~~~~~~

* ``@task.agent``, ``@task.llm``, ``@task.llm_branch``, ``@task.llm_schema_compare``,
  and ``@task.llm_sql`` decorators now accept ``Sequence[UserContent]`` (e.g.,
  ``["Describe this:", ImageUrl(url="...")]``) in addition to ``str`` from the
  decorated callable, enabling vision, audio, and document inputs to pydantic-ai
  agents directly through the TaskFlow decorator path.

Misc
~~~~

* HITL review (``enable_hitl_review=True``) is not supported with a
  ``Sequence[UserContent]`` prompt: the session model types ``prompt`` as a
  string and will raise ``pydantic.ValidationError`` when the agent task tries
  to record the session.
* LLM approval (``require_approval=True``) is not supported with a
  ``Sequence[UserContent]`` prompt: ``LLMApprovalMixin.defer_for_approval``
  now raises ``TypeError`` to prevent the multimodal prompt from being
  stringified into the human review body. Widening the approval body to
  render multimodal content safely is tracked as a follow-up.

0.3.0
.....

Features
~~~~~~~~

* ``Add 'LLMRetryPolicy' to common-ai provider (#65451)``

Bug Fixes
~~~~~~~~~

* ``Update dependencies to fix dependabot alarms in providers.common.ai (#66628)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``[main] CI: Upgrade important CI environment (#66843)``

0.2.0
.....

Features
~~~~~~~~

* ``Add UsageLimits support to common.ai operators (#66248)``

Doc-only
~~~~~~~~

* ``Add Configuration Reference docs page to Common AI provider (#66024)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use contextlib.suppress instead of try-except-pass in providers (#66178)``
   * ``[main] CI: Upgrade important CI environment (#66068)``
   * ``fix: update dependencies to fix dependabot alarms in providers.common.ai (#66244)``
   * ``[main] CI: Upgrade important CI environment (#65933)``
   * ``Providers wave 2026-04-21 (#65614)``
   * ``Providers wave 2026-04-21``

0.1.1
.....

Misc
~~~~

* ``Update dependencies to address Dependabot security alarms in providers.common.ai (#65048)``
* ``Bump vite (#64799)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``[main] CI: Upgrade important CI environment (#65521)``
   * ``Isolate non-provider mypy hooks per distribution with dedicated .build/ venvs (#65492)``
   * ``Simple LLM scenario example based on the Airflow survey data (#65172)``

0.1.0
.....

.. note::
  This release of provider is only available for Airflow 3.0+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.
