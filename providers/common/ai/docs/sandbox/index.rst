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

.. _howto/sandbox:

Sandboxed execution for agents
==============================

An agent that is asked to do open-ended work writes code, and then something has
to run that code. By default that something is the Airflow worker: a skill
script, a hand-written tool that shells out, or generated glue in
:ref:`code mode <code-mode>` all execute on the host that also holds your
connections, your filesystem and your network position, and the code they run
was generated a moment ago from inputs you do not control.

:class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset` gives the
model a disposable workspace for that code instead. It exposes four tools:

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Tool
     - What it does
   * - ``run_command``
     - Runs a shell command. Pipes, redirection, ``&&`` and globs work. A
       non-zero exit is reported as output, not raised, so the model reads
       ``stderr`` and corrects itself.
   * - ``read_file``
     - Reads a text file, head-first, and reports the next ``offset`` so the
       model can page through a long file.
   * - ``write_file``
     - Writes text to a file, creating parent directories.
   * - ``list_directory``
     - Lists a directory. Directories are shown with a trailing ``/``.

The sandbox is provisioned by a
:class:`~airflow.providers.common.ai.sandbox.SandboxBackend` on the model's first
tool call and destroyed when the agent run ends. Two backends ship: a hosted one on
`Modal <https://modal.com/docs/guide/sandbox>`__ for production and Kubernetes,
and a local microVM one on `Docker Sandboxes <https://docs.docker.com/ai/sandboxes/>`__
for development. The four tool names and shapes match pydantic-ai's own sandbox
capabilities, so a model that has seen one already knows this one.

**Adding this toolset gives the agent shell and file operations in a separate
workspace. Every other tool keeps its existing permissions and runs where it ran
before.** Whether that is a new capability depends on what the agent already had:
an agent with a skill script or a shell tool already ran generated code on the
worker, and this is where that code should run instead. An agent that only had
named database operations is being granted a general shell for the first time, in
a workspace with less authority than the worker, and you are choosing to grant it.

Pages in this section
---------------------

.. toctree::
    :maxdepth: 1

    Configuration and lifecycle <configuration>
    Backends <backends>

.. _sandbox-quick-start:

Quick start
-----------

An agent investigating a revenue anomaly. The warehouse is reached through a
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`, so the credential
stays in the task and the model only ever sees rows. The arithmetic happens in a
hosted sandbox with pandas baked into the image, so the sandbox needs no network,
and a bad pivot or a runaway loop costs a small sandbox rather than a worker slot:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_sandbox_toolset.py
    :language: python
    :start-after: [START howto_sandbox_agent_investigation]
    :end-before: [END howto_sandbox_agent_investigation]

The same toolset on the local ``sbx`` backend, for developing on a laptop. Here
the agent maps a vendor file whose columns drift onto a staging schema: it writes a
script, runs it, reads the traceback and fixes it, which is the loop nobody can
write down in advance. Swapping ``SbxSandboxBackend`` for ``ModalSandboxBackend``
is the only change between this Dag and production:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_sandbox_toolset.py
    :language: python
    :start-after: [START howto_sandbox_agent_local]
    :end-before: [END howto_sandbox_agent_local]

Install the Modal extra and authenticate as you would for Modal's own CLI. On a
worker, set ``MODAL_TOKEN_ID`` and ``MODAL_TOKEN_SECRET`` in the environment
instead; nothing is read until the first sandbox is created, so a Dag file that
constructs the backend parses without credentials present:

.. code-block:: bash

    pip install 'apache-airflow-providers-common-ai[modal]'
    modal token new          # writes ~/.modal.toml

What it is for, in practice
---------------------------

The rule is: a model inside a running task writes code you cannot name in advance,
and that code needs a real shell, a real interpreter or packages. These are the
jobs that rule describes, each paired with the version where a sandbox is the wrong
call.

**A vendor drops a CSV nobody has a schema for.** Three date formats, an encoding
error at row 41,000, pandas needed. Every step depends on the previous step's
output, and a bad read of the whole file into memory should exhaust a small
sandbox, not a worker slot shared with other tasks. If the schema is stable and
the mapping is known, this is a plain task with no agent.

**Yesterday's revenue moved and nobody knows why.** The agent queries through a
SQL toolset with a table allowlist, writes the rows into the sandbox, and pivots
with pandas rather than doing arithmetic in tokens. The warehouse credential never
enters the sandbox. If it is always the same five queries, that is
:class:`~airflow.providers.common.ai.operators.llm.LLMOperator` over a fixed SQL
task; if the rows number in the millions, compute in the warehouse rather than
carrying them through the model's context.

**Migrate forty dbt models between warehouse dialects.** The model files go into
the workspace, the image carries ``sqlglot`` and ``sqlfluff``, and the loop of
convert, lint, read errors, fix runs for as long as it takes, fully offline. The
rewritten files are the deliverable, and getting forty files out is the gap
described under :ref:`Getting a result out <sandbox-results>`.

**A failing task's traceback, handed to an agent to propose a fix.** The agent
reproduces the crash against a sample, tries a fix, reruns, and posts a diff for a
person to review. That code ran with the worker's credentials the first time; the
reproduction should not. A written diagnosis with no execution is
``LLMOperator`` reading the log.

**Not a sandbox: flag 200 customers with an open ticket** using two existing
tools. That is :ref:`code mode <code-mode>`: a short loop over registered tools,
no packages, no shell, sub-millisecond start.

**Not a sandbox: run a vendor's PII-scanner image over an S3 prefix.** No model
writes anything. That is ``KubernetesPodOperator``, a fixed payload with templated
arguments in its own pod.

**Not only a sandbox: one team's agent tasks must run off the shared workers.**
That is a where-does-the-task-run requirement, so ``KubernetesExecutor`` with a
pod override, and the sandbox toolset inside the pod if those agents also write
code. The pod protects the cluster from the task; the sandbox protects the task
from its model.

.. _sandbox-boundaries:

Choosing the boundary
---------------------

"Sandboxed" means different things at different layers, and picking the wrong
layer is the most common way to end up with less protection than you think. Four
boundaries exist, from smallest to largest:

.. list-table::
   :widths: 22 33 45
   :header-rows: 1

   * - Boundary
     - What moves inside it
     - What that protects you from
   * - **A tool call**
     - What ``run_command`` and the file tools do. **This toolset.**
     - Model-written code damaging the worker host, reading its files, or
       reaching the network from it.
   * - **The glue between tools**
     - Generated orchestration code, via :ref:`code mode <code-mode>` and the
       Monty interpreter.
     - Generated code touching anything other than the tools you registered.
       Monty runs it in a worker subprocess on the worker host, a language-level
       sandbox rather than an OS one, and every tool it calls executes in the
       task process with the task's authority.
   * - **The agent process**
     - The whole agent loop, its LLM credentials and its message history.
     - The agent's own credentials leaking, and any *other* toolset on the same
       agent. Not available today.
   * - **The whole task**
     - The complete Airflow task, supervisor included, as
       ``KubernetesExecutor`` does.
     - Everything outside the task. Not the task from its own code: the
       agent and what it runs are both inside this boundary.

``SandboxToolset`` is the first row. It relocates exactly its four tools and
nothing else. Boundary size is not a security level on its own: the image, the
credentials you inject, the network policy and the resource limits decide the
actual isolation. Choose the smallest boundary that fits, then configure it.

.. list-table::
   :widths: 48 52
   :header-rows: 1

   * - The agent needs to
     - Reach for
   * - Call operations you can name in advance
     - :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` or
       :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`. Narrow,
       bounded, and the credential stays in the worker rather than reaching the
       model. If you can name the operations, name them.
   * - Chain those tools with glue logic
     - :ref:`code mode <code-mode>`. Monty confines generated code to the tools
       you registered and starts in well under a millisecond, against roughly a
       second to provision a hosted sandbox. Reach past it for a sandbox when the
       model needs something Monty does not have: a shell, ``pip install``, a
       compiled library.
   * - Write and run open-ended code: reshape data with no known schema, install
       a package, or fix its own failing script by reading the traceback
     - ``SandboxToolset``
   * - Produce a large artifact for a downstream task
     - Today, a ``@task`` driving a backend directly, not the agent. That is a
       missing seam in the toolset rather than a recommendation; see
       :ref:`Getting a result out <sandbox-results>`.
   * - A whole task's worth of untrusted work isolated, with no agent involved
     - ``KubernetesPodOperator``
   * - Airflow's own credentials kept away from the agent
     - Not this. Scope the connections the task can see, and give the agent only
       the toolsets it needs. See :ref:`sandbox-security`.

**Code mode and the sandbox compose.** With both enabled, the three file tools
fold into ``run_code`` as callables the generated code can loop over, and
``run_command`` stays a tool the model calls directly, so a shell command is
written as a shell command rather than quoted inside generated Python. Both paths
reach the same sandbox: there is one per agent run whichever way a call arrives.

Why not ``KubernetesPodOperator`` or the ``KubernetesExecutor``?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Because they answer a different question. Both isolate a task from the rest of
your platform. Neither puts a boundary between an agent and the code that agent
writes.

Run an ``AgentOperator`` under the ``KubernetesExecutor`` and the whole task,
agent loop included, gets its own pod. Model-written code still executes inside
that pod, with its service account, its mounted secrets, its connections and its
network position. The pod protects the cluster from the task; it does not protect
the task from what the model decided to run, because from the pod's point of view
that code is the task.

``KubernetesPodOperator`` does contain the work, and its image, command, arguments
and environment are all templated, so it can launch a different payload per run,
and that payload can be a complete agent. The difference is what each one is. The
pod is the environment that hosts the workload, and everything inside it runs with
the pod's authority. ``SandboxToolset`` is a separate workspace that an
already-running workload reaches into repeatedly, with less authority than the
workload itself. Expressing the workspace as a pod means one pod per turn, with
scheduling latency and a lost filesystem between each, or letting the model run
whatever it wants inside the pod that also holds your credentials.

.. list-table::
   :widths: 30 35 35
   :header-rows: 1

   * -
     - ``KubernetesExecutor`` / ``KubernetesPodOperator``
     - ``SandboxToolset``
   * - Protects
     - The cluster, from the task
     - The task and its credentials, from the model's output
   * - Boundary sits
     - Around the whole task, agent included
     - Between the agent and the code it writes
   * - What runs inside is decided
     - Before the process starts
     - By the model, mid-run
   * - Credentials inside
     - The task's own: service account, secrets, connections
     - Only what ``SandboxSpec.env`` names, which is nothing by default

Running an agent in a pod *and* giving it a sandbox is the setup a Kubernetes
deployment usually wants: the pod bounds the task, the sandbox bounds the model.
The ``sbx`` backend cannot be the second half of that, because it needs KVM on the
worker host and an unprivileged pod cannot provide it; that is what the hosted
backend is for.

When to choose it
-----------------

**Choose it when** the work is running code the model wrote, rather than calling
a tool you picked in advance — exploratory analysis, installing a package for one
task, a script the model writes, runs, and fixes from its own traceback. Every
other toolset route answers "call this thing"; this one answers "here is
somewhere to work". This page has the worked scenarios above and
the limitations below to read before designing a Dag around it.

Before reaching for it, check whether the actual need is narrower than that:
``code_mode=True`` is a flag on ``AgentOperator``, not a toolset
— it changes how the model invokes the tools it already has, letting it write
code to call several of them instead of emitting one call per step. It does not
give the agent somewhere to run arbitrary code of its own, and it avoids the
``sbx`` backend's production-readiness, network-isolation, and reclamation
caveats on :doc:`backends` and needs no backend at all — but not the reachability one: the generated code runs in
Monty's deny-by-default sandbox, but the tools it calls still run in the
worker, so a credential-bearing toolset on the same agent stays within reach
whether or not code mode is on. See :ref:`code-mode` and
:ref:`sandbox-boundaries`.

**What it cannot do**

- Only one of its two backends runs on Kubernetes. ``SbxSandboxBackend`` drives
  Docker Sandboxes on the worker host, and its own documentation says to use it
  for local development: it wants the ``sbx`` binary on the host, an
  authenticated Docker account, a one-time ``sbx policy init``, and on Linux KVM
  or nested virtualization — which an unprivileged container cannot provide.
  Production and Kubernetes use
  :class:`~airflow.providers.common.ai.sandbox.modal.ModalSandboxBackend`, a
  hosted backend behind the ``modal`` extra that installs nothing on the worker
  and reclaims a sandbox at its own lifetime if the worker dies. Both implement
  :class:`~airflow.providers.common.ai.sandbox.SandboxBackend`, and a third
  vendor can too.
- It does not contain the agent. Only what these tools do runs in the sandbox;
  the agent loop, the model calls, and every other toolset on the same agent stay
  in the worker with the worker's credentials. It contains model-written code, so
  pairing it with a credential-bearing toolset on the same agent puts the
  credential back within reach. :ref:`sandbox-boundaries` sets this out in full.
- The ``sbx`` backend cannot enforce network isolation on its own. It applies
  ``allow_egress_to`` as a per-sandbox policy rule, but only on top of a
  ``deny-all`` host policy, since a local rule can narrow egress and never
  widen it; ``block_network`` has no per-sandbox enforcement at all. Ask for
  either against a host policy that is not already ``deny-all`` and the
  backend raises rather than silently leaving the sandbox less restricted
  than the spec asked for. ``block_network`` defaults to ``True``, so a bare
  ``SandboxSpec()`` with no arguments already asks for it and is refused
  under the default ``host_network_policy="unknown"``. On Modal the same default
  maps onto the sandbox's own ``block_network`` and is enforced exactly; a
  hostname allowlist there is matched on the TLS handshake name and has to be
  opted into, for the reasons set out on :doc:`backends`.
- Reclamation depends on the backend. A failed teardown is logged as a warning
  rather than raised, deliberately, so that a teardown blip cannot fail a
  finished run. On ``sbx`` nothing else picks up the slack: there is no
  server-side TTL, so a worker killed outright leaves the microVM and its
  workspace directory behind, named ``airflow-sandbox-*`` so an operator can find
  and remove them. On Modal the sandbox ends at its own ``sandbox_timeout``
  whatever became of the worker.
- Nothing survives the run, and a file the agent built can leave only through
  the model's context, which is text-only and capped. Producing an artifact for a
  downstream task is a ``@task`` driving a backend directly today, not the agent;
  :doc:`configuration` has the example.

**A real example.** ``example_sandbox_toolset.py`` in this provider's example
Dags has an agent investigating a revenue anomaly on the Modal backend beside a
``SQLToolset``, the same agent shape on ``sbx`` for a laptop, and a ``@task``
producing a file through a sandbox; all three are on this page and :doc:`configuration`. The two
system tests, ``example_sandbox_toolset_sbx.py`` and
``example_sandbox_toolset_modal.py``, run against a real backend and are
reachable from the System Tests entry in the sidebar.

**Credentials and where it runs.** This route does not end at an
Airflow connection. Airflow puts none of its context, connections, variables or
worker environment into the sandbox; only what you pass through
:class:`~airflow.providers.common.ai.sandbox.SandboxSpec` goes in, and the
credential that provisions the sandbox never enters it. Authorization to the
backend sits outside Airflow — ``sbx login`` and ``sbx policy init`` on the
machine for ``sbx``, or ``MODAL_TOKEN_ID`` and ``MODAL_TOKEN_SECRET`` on the
worker for Modal. Work runs in a per-run microVM on the worker host with
``sbx``, or off the worker entirely in Modal's infrastructure. Its tool calls
act as barriers, as they do for the other
routes that build their own tools; see :ref:`toolset-call-barriers`.

.. _sandbox-limitations:

Limitations
-----------

These apply to every backend. Each is explained in the section it belongs to; this
is the list to read before designing a Dag around an agent with a sandbox.

- **Nothing survives the run**, including across task retries.
  :ref:`Lifecycle <sandbox-lifecycle>`.
- **A file the agent built cannot leave** except through the model's context,
  which is text-only and capped. :ref:`Getting a result out <sandbox-results>`.
- **A credential cannot come from a connection or a secrets backend**; the spec is
  fixed at parse time, and anything injected is readable by the model.
  :ref:`Credentials <sandbox-credentials>`.
- **Do not combine with** ``durable=True`` **or** ``enable_hitl_review=True``.
  Neither is rejected today. :ref:`Lifecycle <sandbox-lifecycle>`.
- **A run that outlives** ``sandbox_timeout`` **fails the task.**
  :ref:`Lifecycle <sandbox-lifecycle>`.
- **The hostname allowlist is a weak control** and refused unless opted into.
  :ref:`Modal <sandbox-backend-modal>`.
- **Commands run as root and** ``workdir`` **is not a jail.**
  :ref:`Modal <sandbox-backend-modal>`.
- **It does not contain the agent.** :ref:`sandbox-security`.

.. _sandbox-security:

Security boundary
-----------------

**The sandbox does not contain the agent.** The agent loop, the model calls and
every other toolset on the same agent still run in the worker process with the
worker's credentials. An agent that has ``SandboxToolset`` *and* a toolset that
can reach connections has a contained code tool sitting beside a credential path
that is not contained: a prompt injection arriving in a queried row can steer the
model into calling the credentialed tool, and that call executes from the worker
with the connection's full grants. Moving the sandbox to a hosted backend changes
none of this.

The controls for that risk are different controls. Give the agent only the
toolsets it needs. Point every connection at a least-privilege role. Prefer a
capability toolset, which lets the model *use* a connection without ever holding
it, over a credential in ``SandboxSpec.env``, which the model can read. And treat
a table allowlist as a guardrail that contains intent rather than a boundary that
contains access.

What the sandbox does contain, verified against a live worker environment carrying
sentinel credentials: nothing Airflow-shaped reaches it. No connection, variable,
Fernet key or cloud credential appeared in the guest's environment, in any process
environment or on its filesystem, and the provisioning token is not usable from
inside even when the vendor's API is on the egress allowlist. Sandboxes cannot reach each
other, so two sandboxes on one agent are isolated from one another as well as from
the worker.
