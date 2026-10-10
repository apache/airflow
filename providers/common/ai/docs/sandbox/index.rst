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

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset` gives an agent a
disposable workspace to run the code it writes. Without one, that code runs on the
Airflow worker: a skill script, a hand-written tool that shells out, or generated glue
in :ref:`code mode <code-mode>` all execute on the host that holds your connections,
your filesystem and your network position, and the code was generated a moment ago
from inputs you do not control.

Use it when a model inside a running task writes code you cannot name in advance, and
that code needs a real shell, a real interpreter or packages. Use something else when:

- **The model only calls operations you can name.** Use
  :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` or
  :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`. They are narrower,
  their results are bounded, and the credential never reaches the model.
- **The model chains tools you registered with a little glue logic**, such as looking
  up 200 customers and flagging the ones with an open ticket. Use
  :ref:`code mode <code-mode>`. It needs no backend and starts in well under a
  millisecond, against roughly a second to provision a hosted sandbox once its image
  is built.
- **The whole task, connections included, has to run off a shared worker.** Use
  ``KubernetesExecutor``, with a pod override for one team's tasks. It isolates the
  task from your platform, not the task from the code its model writes, so the two
  combine (:ref:`sandbox-boundaries`).
- **You are launching a payload whose image and command are known when it starts**,
  such as a vendor's scanner image, or even a complete agent. Use
  ``KubernetesPodOperator``.

.. _sandbox-placement:

Where each piece runs
---------------------

The toolset moves exactly four tools into the sandbox. Everything else on the agent
stays where the task runs:

.. mermaid::

    flowchart TB
        model["Model provider"]
        db[("Warehouse")]
        subgraph host["Worker host or task pod"]
            subgraph task["Airflow task"]
                loop["Agent loop<br/>message history, LLM credential"]
                other["SQLToolset, HookToolset and every other toolset<br/>connection credentials stay here"]
                client["SandboxToolset<br/>backend credential stays here"]
            end
            sbx["sbx microVM<br/>local development only"]
        end
        remote["Modal, OpenSandbox or OpenShell sandbox<br/>run_command, read_file,<br/>write_file, list_directory"]
        model <--> loop
        db <--> other
        loop <--> other
        loop <--> client
        client <--> remote
        client <-.->|"or, on a laptop"| sbx

Commands and files the model sends through these four tools run inside the sandbox,
with only what ``SandboxSpec.env`` puts there. No connection, variable or worker
environment variable goes in, and neither does the credential that provisioned the
sandbox. Tool results come back to the agent loop and from there to the model. The
agent loop, the model calls and every other toolset keep the task's authority, so the
sandbox contains model-written code, not the agent (:ref:`sandbox-security`). Under
``KubernetesExecutor`` the outer box is the task's pod: the pod protects the cluster
from the task, and the sandbox protects the task from its model. An ``sbx`` microVM
cannot run inside an unprivileged pod, so that combination needs Modal, OpenSandbox or OpenShell.

The credential that provisions the sandbox depends on the backend: a ``modal``
connection (``modal_conn_id``; see the :ref:`Modal connection page <howto/connection:modal>`)
for Modal; ``opensandbox_conn_id``, or ``OPEN_SANDBOX_DOMAIN`` and ``OPEN_SANDBOX_API_KEY``,
for OpenSandbox; the worker's ``openshell`` CLI gateway registration for OpenShell;
and the host's ``sbx login`` for ``sbx``.

The four tools:

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

A :class:`~airflow.providers.common.ai.sandbox.SandboxBackend` provisions the sandbox
on the model's first tool call and tears it down when the agent run ends. Four
backends ship: `Modal <https://modal.com/docs/guide/sandbox>`__ (hosted) and
`OpenSandbox <https://open-sandbox.ai/>`__ and
`NVIDIA OpenShell <https://github.com/NVIDIA/OpenShell>`__ (self-hosted) for production and
Kubernetes, and `Docker Sandboxes <https://docs.docker.com/ai/sandboxes/>`__ (``sbx``,
a microVM on the worker host) for local development. The tool names are the ones
pydantic-ai's `Shell <https://pydantic.dev/docs/ai/harness/shell/>`__ and
`FileSystem <https://pydantic.dev/docs/ai/harness/filesystem/>`__ capabilities use.

**Whether this grants something new depends on what the agent already had.** An agent
with a skill script or a shell tool already ran generated code on the worker, and this
is where that code should run instead. An agent that only had named database
operations is being granted a general shell for the first time, in a workspace with
less authority than the worker, and you are choosing to grant it.

Pages in this section
---------------------

.. toctree::
    :titlesonly:
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

Run against a test warehouse with two weeks of daily revenue, in which EMEA web revenue
was seeded to jump on the run date, the task log ends with a summary of the run and the
order of the agent's tool calls. Every ``run_command`` and ``write_file`` ran in the Modal
sandbox; every ``query`` ran in the task:

.. code-block:: text

    LLM run complete: model=claude-sonnet-5, requests=27, tool_calls=27, input_tokens=273753, output_tokens=7137, total_tokens=280890
    LLM run cost: $0.1557896 (USD, best-effort)
    Tool call sequence: list_tables -> get_schema -> get_schema -> query -> write_file -> write_file -> run_command -> run_command -> query -> query -> query -> query -> write_file -> run_command -> query -> query -> query -> query -> run_command -> run_command -> run_command -> run_command -> run_command -> run_command -> run_command -> write_file -> run_command -> final_result

The ``Findings`` the task returned to XCom, with ``summary`` and ``suspected_cause``
shortened here:

.. code-block:: json

    {
      "summary": "Total revenue on 2026-10-05 was $24,864 vs a trailing-7-day average (09-28 to 10-04) of $21,522 — a +15.5% move, exceeding the 10% threshold. Breaking revenue down by region×channel, EMEA/web is the sole driver ...",
      "confidence": "high",
      "suspected_cause": "A step-change in average order value for EMEA/web orders on 2026-10-05 (order count stayed flat at 3, but per-order amount jumped from ~$1,600-$1,733 to $3,040, ~1.75x normal) ...",
      "affected_segments": ["EMEA / web"]
    }

An open-ended investigation can take many model requests. With ``usage_limits`` unset,
pydantic-ai's own default of 50 requests still applies, and a run that needs more fails
the task with ``UsageLimitExceeded: The next request would exceed the request_limit of
50``; one of the two runs of this Dag captured for this page did. Pass ``usage_limits={"request_limit": 100}``, or
a ``cost_limit``, to set the budget yourself.

The same toolset on the local ``sbx`` backend, for developing on a laptop. Here
the agent maps a vendor file whose columns drift onto a staging schema: it writes a
script, runs it, reads the traceback and fixes it, which is the loop nobody can
write down in advance. Swapping ``SbxSandboxBackend`` for ``ModalSandboxBackend``
is the only change between this Dag and production:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_sandbox_toolset.py
    :language: python
    :start-after: [START howto_sandbox_agent_local]
    :end-before: [END howto_sandbox_agent_local]

``host_network_policy="deny-all"`` is there because ``sbx`` cannot block egress per
sandbox. Leave it out and the backend refuses to provision the default spec, which
denies all egress, and the task fails
(:ref:`a failed provisioning fails the task <sandbox-lifecycle>`) with:

.. code-block:: text

    airflow.providers.common.ai.sandbox.base.SandboxTerminalError: SandboxSpec asks for no network egress, but this backend cannot enforce that per sandbox and the host policy has not been declared. Run 'sbx policy init deny-all' on the worker host and pass host_network_policy='deny-all', or pass SandboxSpec(block_network=False) to acknowledge that egress is open.

Install the Modal extra, which also installs the Modal provider, and create a
``modal`` connection with the Modal token id as its login and the token secret as
its password. The backend reads ``modal_default`` unless you pass ``modal_conn_id``;
without that connection it uses the worker's own Modal credentials, as the
:ref:`Modal connection page <howto/connection:modal>` describes. Nothing is read until
the first sandbox is created, so a Dag file that constructs the backend parses without
credentials present:

.. code-block:: bash

    pip install 'apache-airflow-providers-common-ai[modal]'
    airflow connections add modal_default --conn-type modal \
        --conn-login "$MODAL_TOKEN_ID" --conn-password "$MODAL_TOKEN_SECRET"

What it is for, in practice
---------------------------

Four jobs where the model writes code nobody can write down in advance, each paired
with the version where a sandbox is the wrong call.

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
rewritten files are the deliverable, so they leave through ``exports`` when the run
ends (:ref:`sandbox-results`).

**A failing task's traceback, handed to an agent to propose a fix.** The agent
reproduces the crash against a sample, tries a fix, reruns, and posts a diff for a
person to review. That code ran with the worker's credentials the first time; the
reproduction should not. A written diagnosis with no execution is
``LLMOperator`` reading the log.

.. _sandbox-boundaries:

Choosing the boundary
---------------------

"Sandboxed" means different things at different layers, and picking the wrong
layer leaves you with less protection than you think. Four boundaries exist, from
smallest to largest:

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

``SandboxToolset`` is the first row. Boundary size is not a security level on its
own: the image, the credentials you inject, the network policy and the resource
limits decide the actual isolation. Choose the smallest boundary that fits, then
configure it. Two needs are not a boundary at all. A large file for a downstream task
leaves through ``exports`` or through a task that owns the sandbox
(:ref:`sandbox-results`). Keeping Airflow's own credentials away from the agent is done
by scoping the connections the task can see and the toolsets the agent gets
(:ref:`sandbox-security`).

**Code mode and the sandbox compose.** With both enabled, the three file tools
fold into ``run_code`` as callables the generated code can loop over, and
``run_command`` stays a tool the model calls directly, so a shell command is
written as a shell command rather than quoted inside generated Python. Both paths
reach the same sandbox: there is one per agent run whichever way a call arrives.

.. _sandbox-substrates:

What enforces the boundary
^^^^^^^^^^^^^^^^^^^^^^^^^^

Swapping one backend for another changes how the tool-call boundary is enforced and
leaves its contents alone: the same four tools run in the sandbox.

.. list-table::
   :widths: 20 28 26 26
   :header-rows: 1

   * - Enforced by
     - Isolation
     - Where the code runs
     - If the worker dies
   * - Monty, under :ref:`code mode <code-mode>`
     - Language-level confinement. The worker subprocess isolates a crash but is
       not an OS security boundary.
     - A subprocess on the worker host. Every tool the generated code calls runs
       in the task process.
     - Nothing remote to reclaim. The subprocess lives on the worker host
       beside the task.
   * - A container on the worker host (no shipped backend; one you write)
     - Namespaces over the host's shared kernel.
     - The worker host.
     - Up to the backend. Without its own expiry or reaper, the container stays
       until someone removes it.
   * - ``SbxSandboxBackend``
     - A microVM with its own kernel.
     - The worker host.
     - Left running. No server-side lifetime, so the microVM and its workspace
       directory survive.
   * - ``ModalSandboxBackend``
     - Modal's container runtime, which Modal
       `documents as gVisor <https://modal.com/docs/guide/security>`__.
     - Modal's infrastructure, off the worker.
     - Ended by Modal at ``sandbox_timeout``, or ``idle_timeout`` if set.
   * - ``OpenSandboxBackend``
     - The container runtime your OpenSandbox deployment configures, on Docker or
       Kubernetes.
     - Your OpenSandbox server's Docker host or Kubernetes cluster, off the worker.
     - Ended by the OpenSandbox server at ``sandbox_timeout``.
   * - ``OpenShellSandboxBackend``
     - A container confined by Landlock and seccomp; egress goes through a
       per-sandbox supervisor (on the Docker driver, the workload has only a
       loopback network interface).
     - Your OpenShell gateway's Docker or Podman host or Kubernetes cluster, off the
       worker. Verified with the Docker driver only.
     - Left running. No server-side lifetime; sandboxes are labeled
       ``created-by=airflow`` so an operator can reap them.

When a run ends normally, the task calls the backend's ``destroy``. ``sbx`` runs its
removal command and waits up to two minutes for it; Modal, OpenSandbox and OpenShell each
send a termination request and return without waiting for the sandbox to stop. Any of them
can return with the sandbox still present, and none of those cases fails the task. A
SIGKILL, an out-of-memory kill or a lost node skips that teardown entirely, and then
only the last column applies.

Why not ``KubernetesPodOperator`` or the ``KubernetesExecutor``?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Because they answer a different question. Both isolate a task from the rest of
your platform. Neither puts a boundary between an agent and the code that agent
writes.

Run an ``AgentOperator`` under the ``KubernetesExecutor`` and the whole task,
agent loop included, gets its own pod. Model-written code still executes inside
that pod, with its service account, its mounted secrets, its connections and its
network position. From the pod's point of view that code is the task.

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
deployment usually wants, with Modal, OpenSandbox or OpenShell as the backend.

When to choose it
-----------------

**Choose it when** the work is running code the model wrote, rather than calling
a tool you picked in advance: exploratory analysis, installing a package for one
task, a script the model writes, runs, and fixes from its own traceback.

.. _sandbox-limitations:

**What it cannot do**

- **It does not contain the agent.** Pairing it with a credential-bearing toolset
  on the same agent leaves that credential within reach of a steered model.
  :ref:`sandbox-security`.
- **Nothing survives the run** in a sandbox the toolset provisions itself,
  including across task retries. On Modal, a sandbox a task provisions and the agent
  attaches to does; ``sbx``, OpenSandbox and OpenShell cannot be attached to.
  :ref:`Lifecycle <sandbox-lifecycle>`,
  :ref:`A sandbox another task owns <sandbox-attach>`.
- **A file the agent built leaves through** ``exports`` **or a task**, never through
  the model's context, which is text-only and capped.
  :ref:`Getting a result out <sandbox-results>`.
- **A credential handed to the code inside the sandbox comes from a connection
  only when a task provisions the sandbox**, which needs Modal; the toolset's own spec
  is fixed at parse time, and anything injected is readable by the model. :ref:`Credentials <sandbox-credentials>`.
- **It cannot be combined with** ``durable=True``. ``enable_hitl_review=True`` and
  per-tool approval work only when the sandbox is task-owned: ``AgentOperator`` refuses
  HITL review beside a sandbox the toolset provisions itself, and a tool that requires
  approval there fails the task. :ref:`Lifecycle <sandbox-lifecycle>`.
- **A run that outlives** ``sandbox_timeout`` **fails the task.**
  :ref:`Lifecycle <sandbox-lifecycle>`.
- **Network rules are only as strong as the backend.** A backend that cannot enforce
  a network field raises rather than provisioning a looser sandbox, so a bare
  ``SandboxSpec()`` is refused on ``sbx`` until the host policy is declared
  ``deny-all``. Modal's hostname allowlist is a weak control and has to be opted into;
  its address allowlist is enforced properly but cannot serve a package registry whose
  addresses rotate. OpenShell enforces a hostname allowlist on port 443 in its
  per-sandbox supervisor and refuses ``allow_egress_to_cidrs`` and an open network.
  :ref:`Configuring a sandbox <sandbox-configuring>`,
  :ref:`Modal <sandbox-backend-modal>`,
  :ref:`OpenShell <sandbox-backend-openshell>`.
- **On Modal, commands run as root and** ``workdir`` **is not a jail.**
  :ref:`Modal <sandbox-backend-modal>`.
- **The** ``sbx`` **backend is for local development.** It needs the ``sbx`` binary,
  a Docker login and, on Linux, KVM or nested virtualization, so it does not run on
  unprivileged Kubernetes, and a worker killed outright leaves its microVM behind.
  Production and Kubernetes use Modal, OpenSandbox or OpenShell instead.
  OpenShell also has no server-side lifetime; its sandboxes are labeled
  ``created-by=airflow`` so an operator can reap them.
  :ref:`sbx <sandbox-backend-sbx>`.
- **A failed teardown is logged, not raised**, so a teardown blip cannot fail a
  finished run; reclaiming the sandbox is then the backend's lifetime or an
  operator's sweep. :ref:`Cost and operations <sandbox-cost>`.

**A real example.** ``example_sandbox_toolset.py`` in this provider's example Dags
has the two agents in the quick start, a ``@task`` producing a file through a
sandbox, an agent attaching to a task-owned sandbox, and an agent exporting a file,
shown on this page and in :doc:`configuration`. The system tests
``example_sandbox_toolset_sbx.py``, ``example_sandbox_toolset_modal.py``,
``example_sandbox_toolset_opensandbox.py`` and ``example_sandbox_toolset_openshell.py``
run against a real backend and are
reachable from the System Tests entry in the sidebar.

**Credentials and where it runs.** Only what ``SandboxSpec.env`` names enters the
sandbox; the provisioning credential for each backend is listed under
:ref:`sandbox-placement`. Work runs in Modal's infrastructure, on the OpenSandbox or OpenShell
server's runtime (off the worker unless you run the server there), or in a per-run
microVM on the worker host with ``sbx``. Its tool calls act as barriers, as they do
for the other routes that build their own tools; see :ref:`toolset-call-barriers`.

.. _sandbox-other-frameworks:

With another agent framework
----------------------------

A Strands or Google ADK agent can use the same sandbox through ``AirflowTools``
(see :doc:`../frameworks/index`). Outside a Pydantic AI run nothing ends the run for the
toolset, so the task owns the sandbox's life: open the toolset with ``with`` (or
``async with``) around the agent, and the sandbox it provisions is destroyed when the
block ends, however the agent finishes:

.. code-block:: python

    from strands import Agent

    from airflow.providers.common.ai.sandbox.modal import ModalSandboxBackend
    from airflow.providers.common.ai.tools.strands import AirflowTools
    from airflow.providers.common.ai.toolsets import SandboxToolset, SQLToolset
    from airflow.sdk import task

    warehouse = SQLToolset("warehouse", allowed_tables=["ledger"])


    @task
    def reconcile() -> str:
        with SandboxToolset(ModalSandboxBackend()) as sandbox:
            agent = Agent(plugins=[AirflowTools(warehouse, sandbox)])
            return str(agent("Reconcile the September ledger against the warehouse."))

A tool call made before the block or after it raises ``SandboxTerminalError`` saying
the toolset is not open and must be used inside ``with sandbox:`` or
``async with sandbox:``, rather than provisioning a sandbox nothing would destroy. The toolset's own error rules hold as well: a command
that fails is output the model reads, and a sandbox that cannot be provisioned ends the
agent run so the task fails.

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
a table allowlist as a check on which tables the model asks for, not a limit on what
the connection can reach.

Isolation is one of several controls an agent needs, and each has a limit:

.. list-table::
   :widths: 20 25 27 28
   :header-rows: 1

   * - Protection
     - Question it answers
     - Where it stops
     - What provides it
   * - Tool selection and argument checks
     - Which actions can the model request?
     - An allowed action can still be harmful.
     - The toolsets you register, ``allowed_methods`` on ``HookToolset``,
       ``allowed_tables`` on ``SQLToolset``.
   * - Restricted interpreter
     - What can generated glue execute directly?
     - Tools it calls keep their full authority.
     - :ref:`Code mode <code-mode>`.
   * - Process, container or sandbox isolation
     - Which filesystem, processes and host resources can code reach?
     - A credential injected into the sandbox is readable inside it.
     - ``SandboxToolset``; for the whole task, ``KubernetesExecutor`` from the
       ``cncf.kubernetes`` provider.
   * - Identity and entitlement
     - Which data and operations may this workload use?
     - Hiding a password does not remove the power to use it.
     - Least-privilege roles on the connections you pass. Nothing in this
       provider enforces them.
   * - Network policy
     - Which destinations can each component contact?
     - A sandbox's egress policy does not govern worker-side tools or model
       requests.
     - ``SandboxSpec`` network fields, for the sandbox only.
   * - Approval
     - Which effects need sign-off before they happen?
     - Reviewing the final answer does not undo writes made during the run.
     - Keep writes out of the agent and gate the task that makes them.
       ``AgentOperator`` refuses ``enable_hitl_review`` beside a
       ``SandboxToolset`` that provisions its own sandbox; see
       :ref:`Lifecycle <sandbox-lifecycle>`.
   * - Deadlines, resource limits, cleanup
     - What bounds runaway work and orphaned resources?
     - A resource request or a cost report is not an enforced ceiling.
     - Command timeouts, ``sandbox_timeout``, ``usage_limits``.
   * - Audit
     - Can you establish what happened afterwards?
     - A record does not prevent the action.
     - :doc:`LoggingToolset <../toolsets/logging>` and
       :doc:`tracing <../observability>`.

A sandbox with no egress can still leak data. A secret printed inside comes back
through the tool result to the worker and the model's context, and from there it
can reach the agent's output, its message history and any traces you record.
Keeping a credential out of the prompt does not limit its use either. A
``SQLToolset`` never shows the model its password, yet every query the toolset lets
through runs with the connection's grants. Its read-only default and
``allowed_tables`` narrow what reaches the database; the connection's role sets the
limit.
:ref:`toolset-defense-layers` covers the same controls toolset by toolset.

What the sandbox does contain, verified against a live worker environment carrying
sentinel credentials: nothing Airflow-shaped reaches it. No connection, variable,
Fernet key or cloud credential appeared in the guest's environment, in any process
environment or on its filesystem, and the provisioning token is not usable from
inside even when the vendor's API is on the egress allowlist. Sandboxes cannot reach each
other, so two sandboxes on one agent are isolated from one another as well as from
the worker.
