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

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.toolsets.skills.AgentSkillsToolset` loads
`Agent Skills <https://agentskills.io>`__ -- ``SKILL.md`` bundles (instructions,
and optionally scripts and resources) that the model discovers and loads *on
demand*. Only a compact catalog of skill names and descriptions sits in the
prompt until the model decides it needs one, so a large skill library costs few
tokens until used (progressive disclosure).

It is backed by the community `pydantic-ai-skills
<https://github.com/DougTrajano/pydantic-ai-skills>`__ package (MIT). ``pydantic-ai-harness``'s
`Skills <https://github.com/pydantic/pydantic-ai-harness/tree/main/pydantic_ai_harness/skills>`__
capability also loads Agent Skills, as on-demand capabilities, but only their ``SKILL.md``
instructions. :ref:`AirflowSkills <agent-skills-airflow-skills>` builds on it and adds
resources, scripts and ``GitSkills``.
Install the optional extra to use this toolset:

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

.. _agent-skills-restricted:

Restricting the agent
---------------------

This agent can read skills but cannot run any script a skill ships, and files that
match the ``exclude_resources`` patterns stay out of its reach. The example skills ship
no scripts, so the exclusion matters once a skill adds one:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_skills.py
    :language: python
    :start-after: [START howto_operator_agent_skills_restricted]
    :end-before: [END howto_operator_agent_skills_restricted]

The model is offered ``list_skills``, ``load_skill`` and ``read_skill_resource``. A
call to the excluded tool got ``Unknown tool name: 'run_skill_script'. Available
tools: 'list_skills', 'load_skill', 'read_skill_resource'``.

The example skills contain no file the patterns match, so the next run used a copy of
them with ``warehouse.env``, ``secrets/token.txt`` and ``reference.md`` added to
``sql-reporting``. ``load_skill`` listed only ``reference.md`` as a resource, and
reading an excluded file got:

.. code-block:: text

    Resource 'warehouse.env' not found in skill 'sql-reporting'. Available resources: ['reference.md']. Use the exact name from load_skill output.

Without ``max_retries``, each skills tool allows as many corrections as the agent's
``retries``, one by default, and a successful call to that tool resets the count. With
the default, a second refused read in a row failed the run with
``UnexpectedModelBehavior``:

.. code-block:: text

    Tool 'read_skill_resource' exceeded max retries count of 1. Consider raising the retry limit, or see the docs on tool retries: https://pydantic.dev/docs/ai/tools-toolsets/tools-advanced/#tool-retries

With the example's ``max_retries=3``, the same two refused reads went back to the model,
which then read ``reference.md`` and finished the run.

``exclude_resources`` hides files from the resource tools only. A skill's scripts can
still read them, which is why the example excludes ``run_skill_script`` as well.

.. _agent-skills-observability:

Seeing which skills a run used
------------------------------

When the toolset runs inside an Airflow task, it publishes on XCom, under the key
``__AIRFLOW__COMMON_AI_SKILLS__``, every skill it offered the agent: its name and
description, where it came from, and how many times the agent called ``load_skill``,
``read_skill_resource`` and ``run_skill_script`` for it. A local skill names its path on
the worker. A ``GitSkills`` skill names the repository, its path inside the repository,
the ``branch`` you set and the commit that was cloned, so a run that behaves differently
from an earlier one can be traced to a change in the skill between two commits.

The :ref:`AI Model tab <howto/observability:model-tab>` shows this list, loaded skills
first, with a link to the skill's folder at that commit for a repository on GitHub or
GitLab, and says so when the sources held no skills. Calls made before a tool-approval
pause or a human-review regeneration still count, and so do calls a durable retry replays.
Only calls the tool answered count: a denied, retried, failed or interrupted call does not,
while a script that exits with an error still does, since ``run_skill_script`` returns its
output. A ``message_history`` seeded from another task does not add that task's calls, and
calls that a history processor or compaction removes from the transcript are not counted.
``calls`` totals ``calls_by_run``, which keeps the calls of each agent run that made any
under its pydantic-ai run id.
A run resumed from a tool-approval pause and a human-review regeneration each clone the
repository again, so the source shows the commit the latest run cloned. A toolset built by
a function, which the agent wraps in pydantic-ai's ``DynamicToolset``, is built and cloned
again on every model step unless you pass ``DynamicToolset(fn, per_run_step=False)``.
Several skills toolsets on one agent, each wrapped with ``prefixed()`` to keep their tool
names apart, share the same list. Nothing is published when the task sets
``do_xcom_push=False``.

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
- ``max_retries``: How many times the model may correct failed calls to one skills tool
  before the run fails; a successful call to that tool resets the count. Default
  ``None``, the agent's ``retries``. See :ref:`toolset-retry-budget`.

.. _agent-skills-airflow-skills:

The ``AirflowSkills`` capability
--------------------------------

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.skills.AirflowSkills` offers the same Agent Skills as a
pydantic-ai capability, built on ``pydantic-ai-harness``'s ``Skills``. The model sees each
skill's name and description in the prompt and loads the instructions with
``load_capability``. Airflow adds what the harness does not: ``GitSkills`` sources with
credentials from an Airflow connection, a ``read_skill_resource`` tool and a
``run_skill_script`` tool. Both tools refuse a skill until the model has loaded it.

.. code-block:: python

    from airflow.providers.common.ai.operators.agent import AgentOperator
    from airflow.providers.common.ai.skills import AirflowSkills, GitSkills

    AgentOperator(
        task_id="report",
        prompt="Build last week's revenue report.",
        llm_conn_id="pydanticai_default",
        capabilities=[
            AirflowSkills(
                sources=[
                    "./skills",
                    GitSkills(repo_url="https://github.com/my-org/agent-skills", conn_id="github_skills"),
                ],
                exclude=["experimental-skill"],
                exclude_resources=["*.env", "secrets/*"],
            )
        ],
    )

Sources are resolved when a run starts, on the worker, so a Git token is never baked into the
serialized Dag, and cloned repositories are removed when the run ends. Each source is a library
directory holding one sub-directory per skill. When two sources hold a skill of the same name,
the one from the later source is used and a warning is emitted, as with ``AgentSkillsToolset``,
unless their ``SKILL.md`` files are byte-identical. The harness then treats them as one skill
and emits no warning, even when the two skills' other files differ.
``include`` and ``exclude`` take skill names, and naming a skill that no source holds fails the
run.

Where it differs from ``AgentSkillsToolset``:

- The model loads a skill with ``load_capability`` instead of ``load_skill``, and the catalog is
  part of the prompt instead of behind ``list_skills``. Skills are found one level below each
  source directory; ``AgentSkillsToolset`` also finds skills nested deeper.
- A skill's directory is not given to the model as an absolute path, because a cloned directory
  is different on every attempt. A leading ``${SKILL_DIR}/`` or ``${CLAUDE_SKILL_DIR}/`` in the path
  given to ``read_skill_resource`` or ``run_skill_script`` names the skill's own directory, and a
  path without it is relative to the same directory.
- The harness ignores the ``SKILL.md`` header fields that change behavior, such as ``model`` or
  ``allowed-tools``, and emits a warning for them on every run.
- ``read_skill_resource`` reads any UTF-8 text file in the skill except the files the
  ``exclude_resources`` patterns match. ``run_skill_script`` runs files in the skill's root or
  its ``scripts`` directory.
- After the history is compacted, the model has to load a skill again before the tools accept it.

Parameters
~~~~~~~~~~

- ``sources``: Local library directories and/or
  :class:`~airflow.providers.common.ai.skills.GitSkills`. At least one.
- ``include`` / ``exclude``: Skill names to offer, or to leave out. Not both.
- ``exclude_resources``: Glob patterns for files the two tools refuse, added on top of
  ``__pycache__``, ``*.pyc``, ``*.pyo``, ``.DS_Store`` and ``.git``. Each pattern matches the
  skill-relative path or any single component of it, ignoring case.
- ``resources``: Register ``read_skill_resource``. Default ``True``.
- ``scripts``: ``"worker"`` registers ``run_skill_script``, which runs a script as a subprocess of
  the worker with a 30 second timeout; ``None`` registers no script tool. Default ``"worker"``.
  A script that prints more than 50,000 bytes is killed, and the model gets the first 50,000 bytes
  followed by a note that the output was cut off.
  Running scripts in a sandbox is not supported yet, and any other value raises ``ValueError``.
- ``max_retries``: How many times the model may correct failed calls to one of the two tools.
  Default ``None``, the agent's ``retries``. See :ref:`toolset-retry-budget`.

An agent can have only one ``AirflowSkills``; put every source in it.

With ``durable=True``, a script that has run is replayed from the journal on a retry instead of
running again.

Trust model
~~~~~~~~~~~

- The model chooses which script runs and with what arguments. Treat a script's arguments as
  untrusted input, the way an HTTP handler treats a request.
- With ``GitSkills``, scripts can change outside Dag review. Point ``repo_url`` at a trusted
  repository and pin ``branch``.
- A script runs with the worker's own permissions and environment, the same as any task code on that
  worker. It can read an ``AIRFLOW_CONN_*`` variable set on the worker.

Version requirements
~~~~~~~~~~~~~~~~~~~~

``pydantic-ai-harness`` is before its 1.0 release, and each release pins ``pydantic-ai-slim`` to the
exact same version, so installing the ``skills`` extra decides which ``pydantic-ai-slim`` you get.
``AirflowSkills`` needs ``pydantic-ai-harness>=0.66.0``, and reads skills through harness's
``LocalWorkspaceBackend``, which works on POSIX systems only.

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
an endpoint: how this team writes a report, which checks run before a release,
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
  nothing in this toolset routes those scripts into a sandbox, so unless you
  exclude the tool, a skill's scripts run in the worker process with the worker's
  reach. If that is not acceptable, exclude the tool or put the work behind
  ``SandboxToolset`` instead.
- It re-fetches a Git source on every run. ``GitSkills`` is resolved and
  shallow-cloned on the worker when the run starts, and the checkout is deleted
  when it ends; nothing is kept between runs, so a large or slow repository pays
  that clone once per run. A local directory is read in place and
  costs nothing.

**A real example.** The local-directory example at the top of this page comes
from ``example_agent_skills.py``. The two skills it ships, ``aip-tracker`` and
``sql-reporting``, are procedural by nature: neither adds an endpoint the agent
could not already reach. That is the signal you are on the right route.

**Credentials and where it runs.** A local directory needs no credential. A
private repository goes through
:class:`~airflow.providers.common.ai.skills.GitSkills` and its ``conn_id``,
resolved by the Git provider's ``GitHook``; plain ``http://`` is refused when a
``conn_id`` is set, so a credential is never sent in the clear. Cloning, reading
and any script execution happen on the worker.
