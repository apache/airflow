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

.. _sandbox-configuring:

Configuring a sandbox
---------------------

:class:`~airflow.providers.common.ai.sandbox.SandboxSpec` says what a sandbox is
provisioned with. The default denies outbound network access and injects no
environment, and a backend that cannot enforce a field it was given **raises
instead of ignoring it**, so a spec never gives you a false sense of a restriction
being in force.

.. code-block:: python

    from airflow.providers.common.ai.sandbox import SandboxSpec, SbxSandboxBackend
    from airflow.providers.common.ai.toolsets import SandboxToolset

    SandboxToolset(
        SbxSandboxBackend(host_network_policy="allow-all"),
        spec=SandboxSpec(
            env={"HF_TOKEN": "..."},  # only what the generated code legitimately needs
            block_network=False,  # this sandbox may reach the internet
        ),
    )

**Network.** Three modes exist. ``block_network=True``, the default, drops all
outbound traffic including name resolution. On Modal it maps onto the sandbox's
own ``block_network`` flag. ``sbx`` has no per-sandbox enforcement of it at all:
egress there is a host-level ``sbx policy``, so the backend honors the default by
refusing to provision unless the Deployment Manager has declared
``host_network_policy="deny-all"``, which means a bare ``SandboxSpec()`` is refused
under the default ``host_network_policy="unknown"`` rather than silently getting
an open sandbox. ``allow_egress_to`` names hosts the sandbox may reach, and both
backends refuse it until you have said something that makes it enforceable:
``sbx`` applies it as a per-sandbox rule on top of that ``deny-all`` host policy,
since a local rule can narrow egress and never widen it, and Modal matches
hostnames in the TLS handshake, which is weaker than it sounds and has to be
opted into with ``egress_enforcement="sni"`` (see
:ref:`the Modal backend <sandbox-backend-modal>` for exactly what it does and does
not stop). ``block_network=False`` opens outbound access; on Modal the cloud
metadata endpoint and private address ranges stay unreachable even then.

**Packages.** A default sandbox has the Python standard library and no network,
so an agent that reaches for ``pip install`` gets a DNS failure in a few seconds.
Bake what it needs into the image, which needs no egress at all:

.. code-block:: python

    import modal

    ModalSandboxBackend(
        image=modal.Image.from_registry("python:3.12-slim").pip_install("pandas", "pyarrow"),
    )

Or, on Modal, allow exactly the two hosts a Python install needs and accept the
handshake-name caveats: with ``pypi.org`` and ``files.pythonhosted.org`` allowed,
installing pandas took about ten seconds when measured and ``github.com`` stayed
blocked. Tell the model in its instructions what the image already has; the tool
description says whether the sandbox has network access and where its working
directory is, but not which packages are installed.

**Resources.** A default Modal sandbox is a fraction of a core (``nproc`` reports
1) with the standard library and no ``curl``, ``git`` or ``wget``. Set ``cpu`` for
anything heavier. ``memory`` on Modal is a scheduling request, not a ceiling: a
sandbox created with ``memory=512`` allocated 1.5 GiB without complaint when
measured, so do not reach for it to bound what model-written code can consume.
``sbx`` gives a sandbox every host CPU by default and enforces its ``memory`` as a
limit.

**Timeouts.** Three clocks apply. A command has a budget: ``default_command_timeout``
(60 s) when the model does not ask, up to ``max_command_timeout`` (300 s) when it
does. The sandbox has a lifetime: ``sandbox_timeout`` on Modal, 3600 s by default,
and the clock runs through the model's thinking time between tool calls, so size it
against the whole agent run. And Modal can reclaim an idle sandbox after
``idle_timeout``, which defaults to ``None`` because a gap for model generation
looks idle and would take the agent's files with it.

**Two sandboxes on one agent** need ``tool_prefix``, since tool names must be
unique within an agent:

.. code-block:: python

    toolsets = [
        SandboxToolset(
            SbxSandboxBackend(image="python:3.12-slim", host_network_policy="deny-all"),
            tool_prefix="py",
        ),
        SandboxToolset(
            SbxSandboxBackend(image="node:22-slim", host_network_policy="deny-all"),
            tool_prefix="node",
        ),
    ]

That yields ``py_run_command``, ``node_run_command`` and so on. Without a prefix
on at least one of them the run fails at startup. Give the model instructions on
which one to use for what, or it will guess.

.. _sandbox-credentials:

Credentials
-----------

``SandboxSpec.env`` is the only way in. Airflow never populates it: no connection,
variable or worker environment variable reaches a sandbox unless you name it
there, and the credential that *provisions* the sandbox never enters it either.
Modal's token stays on the worker and is used by the client, so code running
inside cannot call Modal as you or create further sandboxes.

Before you put a real secret in ``env``, four things are true of it.

**The model can read it.** Model-generated code can print the environment, and a
tool result travels into the model's context, the task log and whatever the agent
returns to XCom. Treat a credential given to a sandbox as disclosed to the model
and to everything that records the run, and scope it to that one job. If the model
only needs to *use* a system rather than hold its credential, a
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` or
:class:`~airflow.providers.common.ai.toolsets.hook.HookToolset` runs the hook in
the task and returns only the result, which is the better shape whenever the
operations can be named.

**The value is fixed when the Dag file is parsed.** ``toolsets`` is not a
templated field, so a spec cannot carry a Jinja expression, and a connection
lookup written beside it would run in the Dag processor on every parse. An agent's
sandbox therefore cannot take a credential from an Airflow connection or a secrets
backend today. If it has to, the job cannot be an agent's sandbox: drive a backend
from a ``@task``, where you are in ordinary Python at run time and the lookup runs
where it should.

.. code-block:: python

    @task
    def run_with_credential():
        from airflow.providers.common.ai.sandbox import ModalSandboxBackend, SandboxSpec
        from airflow.providers.common.compat.sdk import BaseHook

        token = BaseHook.get_connection("my_api").password  # resolved in the task, not at parse
        backend = ModalSandboxBackend()
        sandbox = backend.create(spec=SandboxSpec(env={"API_TOKEN": token}, block_network=False))
        try:
            ...  # write_file, run_command, read_file
        finally:
            backend.destroy(sandbox)

**On** ``sbx`` **the value is written to the guest filesystem.** The CLI has no
create-time environment flag, so the backend appends ``export`` lines to
``/etc/profile`` inside the microVM, readable by anything in that sandbox for its
whole life. Modal passes the environment at creation and writes no file. Prefer the
hosted backend when the sandbox needs a secret at all.

**Non-string values are refused** at provisioning, with the offending keys named,
rather than failing somewhere less obvious.

.. _sandbox-results:

Getting a result out
--------------------

An agent's result is whatever the model returns: findings, a mapping, a table
small enough to read. That reaches XCom through ``output_type`` like any other
agent output, and the examples above end that way.

What does not come out is a *file* the agent built. The sandbox is destroyed when
the run ends; ``read_file`` is text-only, replacing any byte it cannot decode, so a
parquet or an image cannot survive the round trip at any size; and everything the
model reads is capped, 50 KiB per stream for ``run_command`` and ``max_read_bytes``
(5 MiB) per ``read_file``. Those caps are a budget rather than a transport limit:
a 200 MB file reads out of a live sandbox in under ten seconds on the same path,
but raising the cap costs roughly three times the file size in worker memory to
show the model 50 KiB of it. The toolset does not yet have a seam for an author to
collect what the agent produced; that is a known gap rather than the intended
design.

When the deliverable is a file and the Dag already knows the job, do not use an
agent for it. Drive a backend from a ``@task``: the input goes in through
``write_file``, the output comes back through ``read_file`` with a budget the
worker can afford rather than a model's, and it lands in object storage with only
the location passed downstream. The sandbox needs no network and no credentials
for this, and the task owns its teardown:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_sandbox_toolset.py
    :language: python
    :start-after: [START howto_sandbox_task_artifact]
    :end-before: [END howto_sandbox_task_artifact]

If the agent genuinely has to be the one producing the file, the shape that works
is to give the sandboxed code egress to your bucket and a scoped credential in the
spec, and have it write the object itself, passing the key back through its
output. That widens the default deliberately and is worth a sentence in the Dag
saying why.

.. _sandbox-lifecycle:

Lifecycle, failures and retries
-------------------------------

The sandbox is created lazily on the first tool call, shared by every call in that
agent run, and destroyed when the run ends. A run that never calls a tool never
provisions one, and concurrent runs never share a sandbox. Files written by one
call are visible to later calls in the same run; each ``run_command`` is a fresh
shell, so shell variables and background jobs do not survive between calls.

**A failed command is output, not a failure.** A non-zero exit comes back to the
model with both streams and ``[exit code: N]`` appended, and the model corrects
itself. A recoverable sandbox error, such as a missing path or a file over the
read budget, becomes a bounded retry prompt. Only a terminal failure, such as
rejected credentials or a sandbox that has gone, fails the task, so Airflow's own
retry handles it rather than the model burning its retry budget.

**A command timeout** reports ``[timed out after Ns]`` with the budget that was
actually applied. On Modal the sandbox survives and files written earlier are
still there for the model to inspect. On ``sbx`` the backend has to destroy the
sandbox to be certain the command stopped, and the result then says
``[sandbox was replaced; files from earlier calls are gone]`` so the model knows
to start over. Any other failure that loses the sandbox is reported the same way.

**A run that outlives its sandbox fails the task.** When ``sandbox_timeout`` or
``idle_timeout`` passes mid-run, the next tool call reaches a sandbox that is gone,
which is terminal. This is the normal end of any run longer than its sandbox's
lifetime. Human output review is not part of the run: it starts after the agent
has finished and the sandbox has been destroyed, so a review pause costs no
sandbox time and keeps no files.

**Nothing survives the run.** A task retry starts from an empty sandbox, and so
does every other attempt. Two operator features assume otherwise and must not be
combined with a sandbox today, because neither is rejected:

- ``durable=True`` caches each tool result and replays it on a retry without
  calling the backend, so a replayed ``write_file`` reports success while no
  sandbox exists, and the first call that misses the cache runs against a fresh
  empty one. The model is handed a filesystem that does not match what it was just
  told, and nothing raises.
- ``enable_hitl_review=True`` regenerates after reviewer feedback by starting a
  second agent run, and the first run's sandbox was destroyed when that run ended.
  The regenerated agent gets an empty sandbox while its own history describes
  files it wrote earlier.

Cost and operations
-------------------

**One sandbox spans a whole agent run**, so you pay for the model's thinking time
between tool calls, not only for the seconds your commands run. A default Modal
sandbox is around two cents an hour, so this rarely matters; a data-sized one
(``cpu=8, memory=32768``) is around two dollars an hour, and Modal bills the larger
of what you requested and what you used, which is the second reason ``memory`` is
worth setting deliberately. With ``idle_timeout`` unset, ``sandbox_timeout`` is
the worst-case bill for one run. Fan-out is not the thing to worry about: 25
sandboxes created concurrently from one process all came up in under three
seconds.

**Teardown runs inside the task.** A graceful end and an ordinary exception both
destroy the sandbox. A worker killed outright, an OOM or a lost node never run that
code, and what happens next is the backend's business alone: Modal reclaims the
sandbox at ``sandbox_timeout`` whatever became of the worker; ``sbx`` has no
server-side lifetime, so the microVM and its workspace directory survive with no
owner. A teardown that fails is logged with the sandbox identity and does not fail
the task.

**Finding what a Dag left behind.** Modal sandboxes are named ``airflow-sandbox-*``
and carry an ``airflow_sandbox`` tag plus whatever ``tags`` you pass, and
``modal.Sandbox.list(app_id=..., tags={"dag_id": "my_dag"})`` returns exactly those
still running. Tags are fixed when the backend is constructed, so every sandbox in
a mapped task carries the same ones. When checking whether anything is actually
still running, trust ``Sandbox.list``, which returns only live sandboxes, or
``poll()`` on one you hold: ``None`` means running, and any integer means gone,
including ``137``, which is a stopped sandbox rather than a sick one. Modal's
dashboard lists stopped sandboxes alongside live ones, and the ``Tasks`` count in
``modal app list`` lags by up to about a minute. ``sbx`` sandboxes are named the
same way for an operator sweep.

Backends
--------

.. _sandbox-backend-modal:

Modal (hosted)
^^^^^^^^^^^^^^

:class:`~airflow.providers.common.ai.sandbox.modal.ModalSandboxBackend` runs each
sandbox in Modal, provisioned over the API. Of the backends that ship with the
provider, **this is the one to use in production**, and the only one that runs on
Kubernetes: nothing has to be installed on the worker, model-written code never
executes on the worker host, and Modal reclaims a sandbox at its own lifetime
whether or not the worker survives. It needs the ``modal`` extra and ambient
credentials, as under :ref:`Quick start <sandbox-quick-start>`.

Constructor parameters:

- ``image``: Registry tag for the sandbox image, or a prepared ``modal.Image``
  carrying pre-installed packages. Default ``"python:3.12-slim"``.
- ``app_name``: Modal app the sandboxes are created under. Default
  ``"airflow-sandbox"``.
- ``create_app_if_missing``: Create that app if it does not exist. Default ``True``.
- ``sandbox_timeout``: Maximum lifetime of a sandbox in seconds. Default ``3600``.
  Modal's own default is 300, which is below a plausible agent run.
- ``idle_timeout``: Seconds of inactivity after which Modal reclaims the sandbox,
  or ``None`` to rely on ``sandbox_timeout`` alone. Default ``None``; see
  :ref:`Configuring a sandbox <sandbox-configuring>`.
- ``workdir``: Working directory for commands, created if the image lacks it.
  Default ``"/workspace"``. It is a starting directory, not a jail: absolute paths
  and ``..`` are passed through, and commands run as root, so the model can read
  and write anywhere in the sandbox filesystem. The sandbox boundary is what
  contains that.
- ``cpu``, ``memory``, ``gpu``, ``region``, ``cloud``: passed through to Modal.
  ``None`` lets Modal choose. An unrecognized ``region`` or ``cloud`` fails the
  task rather than falling back.
- ``tags``: Extra Modal tags on every sandbox, e.g. ``{"dag_id": "my_dag"}``.
- ``egress_enforcement``: ``"strict"`` (default) or ``"sni"``. See below.

**Network policy.** ``block_network=True`` maps exactly onto Modal's own
``block_network``, which drops all outbound traffic including DNS. A spec that
names ``allow_egress_to`` is **refused by default**, because Modal cannot combine
an allowlist with ``block_network`` at all, and its hostname allowlist is enforced
by matching the name in the TLS handshake, which means:

- TLS on port 443 to a listed host connects; any other host is refused at once.
- **Non-TLS traffic is dropped, not refused.** A plain HTTP connection to a listed
  host stalls until the client gives up, around two minutes of TCP retries for one
  address, so with the 60 s default command budget the model reads
  ``[timed out after 60s]`` and concludes its command was slow, never that the
  network stopped it.
- **The destination address is not part of the decision.** A connection opened to
  an unrelated address while presenting a listed name is routed to the listed host
  and answered by it: connecting to ``8.8.8.8:443`` with ``pypi.org`` in the
  handshake returns the certificate and content of ``pypi.org``. The allowlist is a
  name-routed egress proxy, not a filter on where packets may go.
- **DNS resolution stays open for every hostname**, listed or not, against
  authoritative servers outside Modal. A freshly generated label under a domain the
  operator controls resolves and returns its answer, so this is a two-way channel.
- A host that shares a **TLS endpoint** with a listed one can be reached by
  presenting the listed name in the handshake and the other in the request. With
  ``pypi.org`` as the only allowed host, a TLS session opened to
  ``files.pythonhosted.org`` while presenting ``pypi.org`` was allowed through and
  answered. Other tenants of the same CDN returned ``421 Misdirected Request``; it is
  co-tenancy of the same TLS endpoint that matters, which you cannot check from
  outside and which can change without notice.

So the allowlist says which name a TLS session may be routed to, and nothing else.
Pass ``ModalSandboxBackend(egress_enforcement="sni")`` to say you accept that and
have the allowlist applied:

.. code-block:: python

    SandboxToolset(
        ModalSandboxBackend(egress_enforcement="sni"),
        spec=SandboxSpec(block_network=True, allow_egress_to=["pypi.org", "files.pythonhosted.org"]),
    )

Entries must be bare hostnames or one leading ``*.`` label; a URL, a ``host:port``,
an address or a single-label name is refused, because Modal applies the list without
checking it and any of those would silently match nothing.

**What the image needs.** ``write_file`` and ``list_directory`` use Modal's own
filesystem API, served by a helper Modal injects into the sandbox, so they need
nothing from the image. ``read_file`` deliberately does not: Modal's read API takes
no length, so it cannot honor a read budget, and a single call on a file that
streams without end (``/dev/zero``, a FIFO, a procfs entry) would pull it into
worker memory unbounded. That tool runs the base class's shell implementation,
which caps the read inside the guest, and the image needs ``stat``, ``head`` and
``base64``. Any Debian or Ubuntu based image, including ``python:*-slim``, has
them.

**Symlinks.** Because ``write_file`` goes through the native API, writing to a path
that is a symlink replaces the link with a regular file and leaves the original
target untouched, where a shell redirect would follow the link.

sbx (Docker Sandboxes, local)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:class:`~airflow.providers.common.ai.sandbox.SbxSandboxBackend` runs each sandbox
in a Docker Sandboxes microVM by driving the ``sbx`` CLI. Each sandbox is a real
microVM with its own kernel.

.. warning::

   **Use this backend for local development, not production.** Docker Sandboxes
   is built for running coding agents against a checkout on your own machine, and
   driving it from an Airflow worker is off-label use. A production worker would
   need the ``sbx`` binary on the host, an authenticated Docker account
   (``sbx login``), a one-time ``sbx policy init``, and on Linux, KVM or nested
   virtualization, which a worker in an unprivileged container cannot provide.

   **Orphans are not reclaimed.** There is no server-side lifetime. If the worker is
   killed outright, the microVM and its workspace directory survive; sandboxes are
   named ``airflow-sandbox-*`` so an operator can find and remove them.

Installing the CLI is a Deployment Manager prerequisite (``brew install
docker/tap/sbx`` or ``winget install Docker.sbx``); the backend needs no Python
dependency. The template image must provide GNU coreutils ``timeout``, ``base64``,
``stat``, ``head``, ``find``, ``mkdir`` and ``dirname``, which any Debian or Ubuntu
based image has.

Constructor parameters:

- ``image``: Container image for the sandbox. Default ``"python:3.12-slim"``.
- ``memory``: Memory limit in binary units. ``sbx`` enforces a 1 GiB minimum.
  Default ``"2g"``.
- ``cpus``: CPUs to allocate. ``None`` (default) uses the ``sbx`` default, which
  is every host CPU.
- ``sbx_path``: Path to the ``sbx`` binary. Default ``"sbx"``.
- ``create_timeout``: Seconds allowed for provisioning; a first-run microVM boot
  plus an image pull can be slow. Default ``600``.
- ``host_network_policy``: What ``sbx policy`` is set to on this host.
  ``"unknown"`` (default) makes ``create`` refuse any spec asking for a network
  guarantee this backend cannot make, and since ``block_network`` defaults to
  ``True`` that includes a bare ``SandboxSpec()``. Set ``"deny-all"`` after running
  ``sbx policy init deny-all``, or ``"allow-all"`` to state that egress is open
  and pass ``SandboxSpec(block_network=False)`` to match.

What differs between the two
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Swapping the backend is one constructor argument, and tool names, spec and prompt
do not change. Four behaviours do, so read them before assuming the same Dag
behaves identically in both places:

- **CPU.** ``sbx`` gives a sandbox every host CPU; Modal defaults to a fraction of
  one, so set ``cpu``.
- **Egress allowlists.** ``sbx`` enforces ``allow_egress_to`` at the host policy
  layer; Modal matches TLS handshake names, which is weaker and has to be opted
  into.
- **Command timeouts.** A timeout destroys an ``sbx`` sandbox and its files; a
  Modal sandbox survives with its files intact.
- **Symlinks.** ``write_file`` through a symlink follows the link on ``sbx`` and
  replaces it on Modal.

Bringing your own backend
^^^^^^^^^^^^^^^^^^^^^^^^^

Any vendor that can create a sandbox, run a command in it and destroy it can plug
in. Subclass :class:`~airflow.providers.common.ai.sandbox.SandboxBackend` in your
own package and pass an instance to ``SandboxToolset``.

**Three methods are required**: ``create``, ``run_command`` and ``destroy``. The
three file operations ship as defaults implemented over ``run_command``, because
reading, writing and listing a file are all expressible as shell commands.
Override them only when the vendor has a native file API:

.. code-block:: python

    from airflow.providers.common.ai.sandbox import (
        SandboxBackend,
        SandboxExecResult,
        SandboxSpec,
    )


    class AcmeSandboxBackend(SandboxBackend):
        name = "acme"

        def create(self, *, spec: SandboxSpec | None = None) -> str:
            return acme_sdk.create_sandbox().id

        def run_command(self, sandbox, command, *, timeout, max_output_bytes):
            r = acme_sdk.exec(sandbox, command, timeout=timeout)
            return SandboxExecResult(exit_code=r.exit_code, stdout=r.stdout, stderr=r.stderr)

        def destroy(self, sandbox) -> None:
            acme_sdk.delete_sandbox(sandbox)

        # Optional: inherited from SandboxBackend unless the vendor has
        # something better than shelling out.
        def read_file(self, sandbox, path, *, max_bytes) -> bytes:
            return acme_sdk.download(sandbox, path, limit=max_bytes)

Four rules for an implementation:

- Constructors run at Dag-parse time, so resolve credentials lazily, on first use.
- ``destroy`` must be idempotent; destroying an already-gone sandbox is not an error.
- Raise ``SandboxTerminalError`` when retrying cannot help and ``SandboxError``
  when it might. The first fails the task for Airflow to retry; the second
  becomes a bounded prompt back to the model.
- If you cannot enforce something the ``SandboxSpec`` asks for, **raise**. Never
  provision a weaker sandbox than the Dag author asked for.

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

Parameters
----------

- ``backend``: The backend that provisions and drives the sandbox.
- ``spec``: What to provision it with. Defaults to no environment and no egress.
- ``default_command_timeout``: Seconds for a ``run_command`` the model did not
  put a timeout on. Default ``60``.
- ``max_command_timeout``: Hard ceiling for any single command, including a
  model-supplied ``timeout_seconds``. Default ``300``.
- ``max_output_lines`` / ``max_output_bytes``: Caps per output stream and per
  file read, whichever is hit first. Defaults ``2000`` and 50 KiB. Command
  output keeps the **tail**, where errors and the exit status live; file reads
  keep the head and report a continuation offset.
- ``max_read_bytes``: Largest file ``read_file`` will transfer. Default 5 MiB;
  larger files are refused with a hint to slice them in the shell.
- ``tool_prefix``: Prefix for the four tool names. Needed when one agent has
  more than one ``SandboxToolset``.

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
