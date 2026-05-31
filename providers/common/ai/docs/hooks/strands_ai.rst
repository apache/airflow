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

.. _howto/hook:strands_ai:

StrandsHook
===========

Use :class:`~airflow.providers.common.ai.hooks.strands_ai.StrandsHook` and its
subclasses to run multi-turn agents via `Strands Agents <https://strandsagents.com/>`__.

The hook implements :class:`~airflow.providers.common.ai.hooks.base.BaseAIHook`, so
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator` resolves it from the
connection ``conn_type``.

Current implementations:

* :class:`~airflow.providers.common.ai.hooks.strands_ai.StrandsGeminiHook`
  (``conn_type`` ``strands-gemini``) — Google Gemini models

Install the optional dependency:

.. code-block:: bash

    pip install 'apache-airflow-providers-common-ai[strands]'

.. seealso::
    :ref:`Strands Agents connection <howto/connection:strands-gemini>`

Basic Usage
-----------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_strands.py
    :language: python
    :start-after: [START howto_hook_strands_basic]
    :end-before: [END howto_hook_strands_basic]

Toolsets
--------

Pass :class:`~airflow.providers.common.ai.hooks.base.BaseToolset` instances (for example
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`) via ``AgentRunRequest.toolsets``.
The hook converts each tool to a Strands-native callable.

Skills
------

Pass skill paths or :class:`~airflow.providers.common.ai.hooks.base.SkillSpec` objects via
``AgentRunRequest.skills`` or ``AgentOperator.skills``. The hook attaches Strands
``AgentSkills`` as a plugin so the agent loads specialized instructions on demand.

For filesystem skills, set the Airflow Variable ``strands_skill_path`` to a directory
containing ``SKILL.md`` (Agent Skills spec). A sample layout ships under
``example_dags/skills/`` in this provider package:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_strands.py
    :language: python
    :start-after: [START howto_operator_strands_skills_path]
    :end-before: [END howto_operator_strands_skills_path]

For inline programmatic skills, use :class:`~airflow.providers.common.ai.hooks.base.SkillSpec`:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_strands.py
    :language: python
    :start-after: [START howto_operator_strands_skills]
    :end-before: [END howto_operator_strands_skills]

Inline skills can also be combined with toolsets:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_strands.py
    :language: python
    :start-after: [START howto_operator_strands_skill_spec]
    :end-before: [END howto_operator_strands_skill_spec]

When a skill includes resource files (``scripts/``, ``references/``, ``assets/``), provide
tools such as ``file_read`` and ``shell`` from ``strands-agents-tools`` via ``toolsets`` or
plain callables so the agent can access them.

Configure the ``AgentSkills`` plugin via operator ``skills_params`` (for example
``strict``, ``max_resource_files``, ``state_key``).

Limitations
-----------

- Durable execution (``durable=True``) is not supported for Strands hooks.
- Usage limits are not yet supported for Strands hooks. Support is expected in
  an upcoming Strands release.
