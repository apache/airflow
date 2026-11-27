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

Agent Skills: ``AirflowSkills``
===============================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.skills.AirflowSkills` loads
`Agent Skills <https://agentskills.io>`__ -- ``SKILL.md`` bundles (instructions,
and optionally scripts and resources) that the model discovers and loads *on
demand*. Only a compact catalog of skill names and descriptions sits in the
prompt until the model decides it needs one, so a large skill library costs few
tokens until used (progressive disclosure).

It is a pydantic-ai capability built on ``pydantic-ai-harness``'s
`Skills <https://github.com/pydantic/pydantic-ai-harness/tree/main/pydantic_ai_harness/skills>`__.
The harness loads a skill's ``SKILL.md`` instructions; ``AirflowSkills`` adds resources, scripts and
``GitSkills``. It replaces the deprecated
:ref:`AgentSkillsToolset <agent-skills-deprecated-toolset>`; see :ref:`agent-skills-migration`
to move an existing Dag over.
Install the optional extra to use it:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[skills]"

Each source is a local directory or a connection-resolved
:class:`~airflow.providers.common.ai.skills.GitSkills`. Sources are resolved when
the agent starts a run, on the worker -- never while the Dag processor
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
    review and versioning. Point ``GitSkills`` at a trusted repository, set
    ``branch`` to a tag, because a branch name moves, and treat skill contents as code that runs in
    your environment.

.. _agent-skills-restricted:

Restricting the agent
---------------------

This agent can load skills and read their resources, but no ``run_skill_script`` tool is
registered, so it cannot run a script a skill ships. Files that match the ``exclude_resources``
patterns are refused by ``read_skill_resource``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_skills.py
    :language: python
    :start-after: [START howto_operator_agent_skills_restricted]
    :end-before: [END howto_operator_agent_skills_restricted]

``exclude_resources`` limits the two skill tools only. A script that does run can still read any
file in the skill's directory, which is why the example sets ``scripts=None`` as well.

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
  ``allowed-tools``, and lists them in a ``UserWarning`` when a run starts. With Python's default
  warning filter, the same warning is shown only once in a process.
- ``read_skill_resource`` reads UTF-8 text files of up to 1,000,000 bytes in the skill, except the
  files the ``exclude_resources`` patterns match; a larger file is refused. It returns text, except
  that ``.json``, ``.yaml`` and ``.yml`` files are returned parsed (content that does not parse is
  returned as text), so the model and the durable journal see the parsed value.
- ``run_skill_script`` runs a file in the skill's root or its ``scripts`` directory if the
  file has a ``.py``, ``.sh``, ``.bash`` or ``.zsh`` suffix or is executable. It refuses
  ``SKILL.md`` and ``__init__.py``, and a script path that ``exclude_resources`` matches.
  An argument name must start with a letter and contain only letters, digits, ``-`` and ``_``;
  the tool refuses any other name. A script that starts with a shebang runs under the interpreter it
  names, and if the worker does not have that interpreter the tool refuses the script with an error
  that says so, even for a ``.py`` file. A ``.py`` file without a shebang runs under the worker's own
  Python.
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
  When a script prints more than 50,000 bytes, the model gets the first 50,000 bytes followed by a
  note that the output was cut off, and the script is killed if it is still running. If it had already
  exited and a background process printed the excess, the script's exit code is reported too.
  A script that runs past the timeout is killed, and the model gets the output it had printed so far,
  followed by a note that it timed out.
  The call returns when the script itself exits, with the script's own exit code. A process the script
  started in the background can keep printing for 2 more seconds, and then it is killed along with the
  script's process group. A process that left the group, for example with ``setsid``, is not killed, but
  the worker closes its end of the output pipe, so that process gets a broken-pipe error when it next writes.
  Running scripts in a sandbox is not supported yet, and any other value raises ``ValueError``.
- ``inherit_env``: Names of worker environment variables a script receives, besides ``PATH``,
  ``HOME``, ``LANG``, ``LC_ALL`` and ``LC_CTYPE``, which it always gets. A name is looked up on the
  worker when the script runs, so no value is stored in the Dag file or its serialized form. A name
  the worker has not set is skipped, and its name, never a value, is logged as a warning when the run
  starts. Default: none.
- ``max_retries``: How many times the model may correct failed calls to one of the two tools.
  Default ``None``, the agent's ``retries``. See :ref:`toolset-retry-budget`.

An agent can have only one ``AirflowSkills``; put every source in it. Its tools belong to a toolset
with the id ``airflow_skills``. With ``durable=True``, another toolset with that id, or a second
``AirflowSkills``, fails when the Dag loads. An ``AirflowSkills`` nested inside another capability,
or built by a capability function, is not seen then and fails when the run starts. Without
``durable``, a second ``AirflowSkills`` fails when the run starts.

With ``durable=True``, a script that has run is replayed from the journal on a retry instead of
running again.

When a run cannot start
~~~~~~~~~~~~~~~~~~~~~~~

A configuration that cannot load its skills raises
:class:`~airflow.providers.common.ai.exceptions.SkillsConfigurationError`, and the task fails
without retrying, because every attempt would fail the same way. That covers a missing
``pydantic-ai-harness`` or GitPython, a local source that does not exist, an ``include`` or
``exclude`` naming a skill no source holds, a ``GitSkills`` URL or path that is not valid, and a second
``AirflowSkills`` on an agent that is not ``durable``.
A clone that fails raises ``RuntimeError`` instead, and the task retries as often as its
``retries`` allows.

Trust model
~~~~~~~~~~~

- The model chooses which script runs and with what arguments. Treat a script's arguments as
  untrusted input, the way an HTTP handler treats a request.
- With ``GitSkills``, scripts can change outside Dag review. Point ``repo_url`` at a trusted
  repository and set ``branch`` to a tag, because a branch name moves.
- A script runs with the worker's own permissions, the same as any task code on that worker, but
  receives only ``PATH``, ``HOME``, ``LANG``, ``LC_ALL``, ``LC_CTYPE`` and the variables
  ``inherit_env`` names. It cannot read the worker's ``AIRFLOW_CONN_*`` variables from its
  environment, though it can still read any file the worker's user can.

Version requirements
~~~~~~~~~~~~~~~~~~~~

``pydantic-ai-harness`` is before its 1.0 release, and each release pins ``pydantic-ai-slim`` to the
exact same version, so installing the ``skills`` extra decides which ``pydantic-ai-slim`` you get.
``AirflowSkills`` needs ``pydantic-ai-harness>=0.66.0``, and reads skills through harness's
``LocalWorkspaceBackend``, which works on POSIX systems only.

``AirflowSkills`` keeps each run's skill directories in the memory of the worker process. It has been
tested with Airflow's own durable execution (``durable=True``). A Temporal activity can run in a
different process, which does not hold that state. The DBOS and Prefect engines of pydantic-ai have
not been tested.

.. _agent-skills-migration:

Migrating from ``AgentSkillsToolset``
-------------------------------------

``AgentSkillsToolset`` is deprecated and emits an ``AirflowProviderDeprecationWarning`` when you create
it. It will be removed in a later release of this provider, and ``pydantic-ai-skills``, the package it
needs, will then leave the ``skills`` extra. New code should use ``AirflowSkills``.

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - ``AgentSkillsToolset``
     - ``AirflowSkills``
   * - ``AgentSkillsToolset(sources=[...])``
     - ``AirflowSkills(sources=[...])``
   * - ``toolsets=[...]`` on the operator
     - ``capabilities=[...]``
   * - ``exclude_tools={"run_skill_script"}``
     - ``scripts=None``
   * - ``exclude_tools={"read_skill_resource"}``
     - ``resources=False``
   * - ``list_skills`` and ``load_skill`` tools
     - ``load_capability``; the catalog is part of the prompt
   * - ``exclude_resources=[...]``, ``max_retries=...``
     - The same parameters
   * - A script inherits the worker's whole environment
     - A script receives only ``PATH``, ``HOME``, ``LANG``, ``LC_ALL`` and ``LC_CTYPE``. Name any other
       variable it needs in ``inherit_env``.
   * - A skill nested more than one level below a source
     - Not found, and no warning or error says so. Check your layout: each skill directory must sit
       directly in a source directory.

``exclude_tools`` has no other counterpart: ``AirflowSkills`` has no ``list_skills`` or ``load_skill``
tool to hide. To offer fewer skills, use ``include`` or ``exclude``.

.. code-block:: python

    # Before
    AgentOperator(
        task_id="report",
        prompt="Build last week's revenue report.",
        llm_conn_id="pydanticai_default",
        toolsets=[AgentSkillsToolset(sources=["./skills"], exclude_tools={"run_skill_script"})],
    )

    # After
    AgentOperator(
        task_id="report",
        prompt="Build last week's revenue report.",
        llm_conn_id="pydanticai_default",
        capabilities=[AirflowSkills(sources=["./skills"], scripts=None)],
    )

How ``AirflowSkills`` behaves differently is listed under
:ref:`AirflowSkills <agent-skills-airflow-skills>` above, from where skills are found to what a
script path means.

.. warning::

    The skills telemetry is not migrated yet. The skills XCom (``__AIRFLOW__COMMON_AI_SKILLS__``) and
    the skills list in the :ref:`AI Model tab <howto/observability:model-tab>` come from
    ``AgentSkillsToolset``, and the tab counts a skill as loaded when the agent called ``load_skill``.
    ``AirflowSkills`` publishes no skills XCom and loads a skill with ``load_capability``, so a task
    that uses it shows no skills in the tab.

.. _agent-skills-deprecated-toolset:

``AgentSkillsToolset`` (deprecated)
-----------------------------------

.. deprecated::
    Use :ref:`AirflowSkills <agent-skills-airflow-skills>` instead. See :ref:`agent-skills-migration`.

:class:`~airflow.providers.common.ai.toolsets.skills.AgentSkillsToolset` is a pydantic-ai toolset
backed by the community `pydantic-ai-skills <https://github.com/DougTrajano/pydantic-ai-skills>`__
package (MIT), which dropped the class it wraps in 2.0, so the ``skills`` extra keeps it below 2. It
is passed in the operator's ``toolsets=`` list:

.. code-block:: python

    AgentOperator(
        task_id="report",
        prompt="Build last week's revenue report.",
        llm_conn_id="pydanticai_default",
        toolsets=[
            AgentSkillsToolset(
                sources=[
                    "./skills",
                    GitSkills(repo_url="https://github.com/my-org/agent-skills", conn_id="github_skills"),
                ],
                exclude_tools={"run_skill_script"},
                exclude_resources=["*.env", "secrets/*"],
                max_retries=3,
            )
        ],
    )

The model is offered ``list_skills``, ``load_skill``, ``read_skill_resource`` and
``run_skill_script``, minus the tools in ``exclude_tools``. ``exclude_resources`` hides files from
the resource tools only; a skill's scripts can still read them, so pair it with
``exclude_tools={"run_skill_script"}`` when the files are sensitive.

.. _agent-skills-observability:

Seeing which skills a run used
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When ``AgentSkillsToolset`` runs inside an Airflow task, it publishes on XCom, under the key
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

``AgentSkillsToolset`` parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Using Agent Skills with other frameworks
----------------------------------------

``AirflowSkills`` is a pydantic-ai capability, so it can also be passed to the ``capabilities=``
list of a plain ``pydantic_ai.Agent`` you build yourself, not just ``AgentOperator``.

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
:class:`~airflow.providers.common.ai.skills.AirflowSkills` makes it
discoverable. See :ref:`agent-skills` for the layout.

**What it cannot do**

- ``exclude_resources`` does not hide a file from the skill's own scripts. It
  keeps matches out of resource discovery and out of ``read_skill_resource``,
  and it does not stop ``run_skill_script`` from reading them off disk. For
  genuinely sensitive files, pair it with ``scripts=None``.
- It does not move script execution anywhere safer. Nothing in ``AirflowSkills``
  routes scripts into a sandbox, so unless you pass ``scripts=None``, a skill's
  scripts run as a subprocess of the worker with the worker's reach. If that is
  not acceptable, pass ``scripts=None`` or put the work behind ``SandboxToolset``
  instead.
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
