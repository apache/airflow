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

.. _agent-skills:

Agent Skills: ``AgentSkillsToolset``
====================================

:class:`~airflow.providers.common.ai.toolsets.skills.AgentSkillsToolset` loads
`Agent Skills <https://agentskills.io>`__ -- ``SKILL.md`` bundles (instructions,
and optionally scripts and resources) that the model discovers and loads *on
demand*. Only a compact catalog of skill names and descriptions sits in the
prompt until the model decides it needs one, so a large skill library costs few
tokens until used (progressive disclosure).

It is backed by the community `pydantic-ai-skills
<https://github.com/DougTrajano/pydantic-ai-skills>`__ package (MIT); native
progressive disclosure is in flight upstream in `pydantic/pydantic-ai#5230
<https://github.com/pydantic/pydantic-ai/pull/5230>`__. Install the optional
extra to use it:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[skills]"

Each source is a local directory or a connection-resolved
:class:`~airflow.providers.common.ai.skills.GitSkills`. Sources are resolved when
the agent enters the toolset, on the worker -- never while the Dag processor
parses the file -- so a Git token is never baked into the serialized Dag, and
cloned repositories are removed when the run ends.

A local directory of ``SKILL.md`` bundles:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_skills.py
    :language: python
    :start-after: [START howto_operator_agent_skills_local]
    :end-before: [END howto_operator_agent_skills_local]

A Git repository, with credentials from an Airflow connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_skills.py
    :language: python
    :start-after: [START howto_operator_agent_skills_git]
    :end-before: [END howto_operator_agent_skills_git]

For a private repository, point ``conn_id`` at a
:doc:`git connection <apache-airflow-providers-git:connections/git>`; credentials
are resolved through the Git provider's ``GitHook`` (an HTTPS token in the
connection password, or an SSH key in the connection's extra). A plain ``http://``
URL with ``conn_id`` is rejected so a credential is never sent in cleartext, and a
``repo_url`` that embeds a username/password is rejected (use ``conn_id``). After
cloning, the credential is stripped from the checkout's ``.git/config``. As with
any ``git clone``, the worker's own git configuration (credential helpers, SSH
agent) may still apply, so run workers without ambient git credentials if you
need strict isolation.

.. warning::

    Skill bundles can contain scripts that the agent may run on the worker via
    the ``run_skill_script`` tool. For a remote source, anyone who can modify the
    repository can introduce code that executes on your worker, outside Dag
    review and versioning. Point ``GitSkills`` at a trusted repository, pin
    ``branch`` to a trusted ref, and treat skill contents as code that runs in
    your environment.

Parameters
----------

- ``sources``: List of skill sources -- local directory paths and/or
  :class:`~airflow.providers.common.ai.skills.GitSkills`.
- ``exclude_tools``: Optional set of skill tool names to hide from the agent
  (e.g. ``{"run_skill_script"}`` to disable on-worker script execution).
- ``exclude_resources``: Optional glob patterns to exclude from resource
  discovery, added on top of the built-in defaults (``__pycache__``, ``*.pyc``,
  ``*.pyo``, ``.DS_Store``, ``.git``). A skill exposes every readable text file
  it contains as a resource; these patterns keep matched files out of the
  resource list and the ``read_skill_resource`` tool (e.g.
  ``["*.env", "secrets/*"]``). Each pattern matches the full skill-relative path
  or any single path component. This hides files from resource discovery only --
  it does not stop a skill's ``run_skill_script`` from reading them off disk, so
  pair it with ``exclude_tools={"run_skill_script"}`` when the files are
  genuinely sensitive.

Using Agent Skills with other frameworks
----------------------------------------

``AgentSkillsToolset`` is a standard pydantic-ai toolset, so it also works with a
plain ``pydantic_ai.Agent`` you build yourself, not just ``AgentOperator``.

Because Agent Skills is a cross-framework format, the connection handling is also
reusable through :func:`~airflow.providers.common.ai.skills.resolve_skills`, which
resolves sources to local ``SKILL.md`` directories that any loader accepts:

.. code-block:: python

    from airflow.providers.common.ai.skills import GitSkills, resolve_skills

    sources = ["./skills", GitSkills(repo_url="https://github.com/org/skills", conn_id="github_skills")]
    with resolve_skills(sources) as dirs:
        # LangChain DeepAgents
        agent = create_deep_agent(model="openai:gpt-5.4", skills=dirs)
        # ...or Strands
        agent = Agent(plugins=[AgentSkills(skills=dirs)])

``resolve_skills`` needs the Git provider (for ``GitSkills``) but not pydantic-ai,
and removes any cloned directories when the ``with`` block exits.

When to choose it
-----------------

**Choose it when** what the agent is missing is procedural knowledge rather than
an endpoint — how this team writes a report, which checks run before a release,
what the house conventions are. A skill is a directory of instructions and
optional scripts, and
:class:`~airflow.providers.common.ai.toolsets.skills.AgentSkillsToolset` makes it
discoverable. See :ref:`agent-skills` for the layout.

**What it cannot do**

- ``exclude_resources`` does not hide a file from the skill's own scripts. It
  keeps matches out of resource discovery and out of ``read_skill_resource``,
  and the parameter's documentation says plainly that it does not stop
  ``run_skill_script`` from reading them off disk. For genuinely sensitive files,
  pair it with ``exclude_tools={"run_skill_script"}``. (The parameter needs
  ``pydantic-ai-skills>=1.2.0``, which the ``skills`` extra already pins.)
- It does not move script execution anywhere safer. The toolset's own wording for
  ``exclude_tools`` calls ``run_skill_script`` "on-worker script execution", and
  nothing in this toolset routes those scripts into a sandbox — so unless you
  exclude the tool, a skill's scripts run in the worker process with the worker's
  reach. If that is not acceptable, exclude the tool or put the work behind
  ``SandboxToolset`` instead.
- It re-fetches a Git source on every run. ``GitSkills`` is resolved and
  shallow-cloned on the worker when the run starts, and the checkout is deleted
  when it ends; nothing is kept between runs, so a large or slow repository pays
  that clone once per run. A local directory is read in place and
  costs nothing.

**A real example.** ``example_agent_skills.py`` loads skills from a local
directory:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_skills.py
    :language: python
    :start-after: [START howto_operator_agent_skills_local]
    :end-before: [END howto_operator_agent_skills_local]

The two skills it ships, ``aip-tracker`` and ``sql-reporting``, are procedural by
nature: neither adds an endpoint the agent could not already reach. That is the
signal you are on the right route.

**Credentials and where it runs.** A local directory needs no credential. A
private repository goes through
:class:`~airflow.providers.common.ai.skills.GitSkills` and its ``conn_id``,
resolved by the Git provider's ``GitHook``; plain ``http://`` is refused when a
``conn_id`` is set, so a credential is never sent in the clear. Cloning, reading
and any script execution happen on the worker.
