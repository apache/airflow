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

.. _dq:agents:

Generating rules with an LLM
==============================

Writing a :class:`~airflow.providers.common.dataquality.rules.RuleSet` by hand for every table doesn't scale.
An LLM can propose one from a table's column definitions instead, given the check catalog as
context.

The ``dataquality-rule-authoring`` skill
----------------------------------------

This provider ships an `Agent Skill <https://agentskills.io>`__ at
``airflow/providers/common/dataquality/skills/dataquality-rule-authoring/``: a ``SKILL.md`` documenting the
``RuleSet``/``DQRule`` fields and check catalog, plus a generated JSON Schema
(``references/ruleset.schema.json``) for validation.

Point ``common.ai``'s :doc:`AgentSkillsToolset <apache-airflow-providers-common-ai:toolsets>` at
it, and give the model ``output_type=RuleSet`` so pydantic-ai validates -- and self-corrects --
its output before the task completes:

.. exampleinclude:: /../src/airflow/providers/common/dataquality/example_dags/example_dq_llm_generated_ruleset.py
    :language: python
    :start-after: [START howto_task_dq_generate_ruleset_with_llm]
    :end-before: [END howto_task_dq_generate_ruleset_with_llm]

Requires ``apache-airflow-providers-common-ai[skills]`` and a configured ``llm_conn_id``.

Wiring the result into a check
---------------------------------

``@task.dq_check`` can leave ``ruleset=`` unset and return the LLM task's result at execution
time instead:

.. exampleinclude:: /../src/airflow/providers/common/dataquality/example_dags/example_dq_llm_generated_ruleset.py
    :language: python
    :start-after: [START howto_decorator_dq_check_llm_runtime_ruleset]
    :end-before: [END howto_decorator_dq_check_llm_runtime_ruleset]
