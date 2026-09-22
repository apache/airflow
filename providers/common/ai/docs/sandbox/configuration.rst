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

Sandbox configuration and lifecycle
===================================

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

**Network.** ``block_network=True``, the default, drops all outbound traffic
including name resolution. On Modal it maps onto the sandbox's
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
not stop). ``allow_egress_to_cidrs`` names address ranges instead, and on Modal it
is enforced on the destination address for any port, so it needs no opt-in; it is
the mode for one service at a fixed public IPv4 address, and ``sbx`` refuses it. A
private address is unreachable from a hosted sandbox whether or not it is listed.
The two lists can be set together, and traffic matching either is allowed, which
weakens the address list on port 443.
``block_network=False`` opens outbound access; on Modal the cloud metadata
endpoint and private address ranges stay unreachable even then.

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
agent output, and the examples on :doc:`index` end that way.

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

**A failed provisioning fails the task.** The model has no input into ``create``:
it takes only the spec, which is fixed in the Dag file. So whatever a backend
raises while provisioning is treated as terminal, even a ``SandboxError`` that
would have been a model retry from a tool call. The task fails and Airflow's own
retry attempts the provisioning again, instead of the model spending its retry
budget on an image tag or a credential it cannot see.

**Nothing survives the run.** A task retry starts from an empty sandbox, and so
does every other attempt. Two operator features assume otherwise, and
``AgentOperator`` refuses each of them at construction when any toolset, including
one nested inside ``.prefixed()``, a combined toolset or a ``Toolset`` capability,
is a ``SandboxToolset``:

- ``durable=True`` caches each tool result and replays it on a retry without
  calling the backend, so a replayed ``write_file`` would report success while no
  sandbox exists, and the first call that misses the cache would run against a
  fresh empty one.
- ``enable_hitl_review=True`` regenerates after reviewer feedback by starting a
  second agent run, and the first run's sandbox was destroyed when that run ended.
  The regenerated agent would get an empty sandbox while its own history describes
  files it wrote earlier.

The two ways out are dropping the flag, or moving the sandbox work into its own
task and keeping the durable or reviewed agent free of sandbox tools. A toolset
resolved per run from a callable cannot be inspected when the operator is built,
so it is the one composition the check does not see.

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

``SandboxToolset`` parameters
-----------------------------

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
