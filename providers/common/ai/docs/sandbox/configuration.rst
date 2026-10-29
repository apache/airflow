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

Configure a sandbox
===================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

A :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset` gives an agent
a sandbox with no network access and none of Airflow's credentials, and deletes it
when the agent run ends. To change that:

- To let the sandbox reach the network or give it environment variables, pass a
  :class:`~airflow.providers.common.ai.sandbox.SandboxSpec`
  (:ref:`sandbox-configuring`, :ref:`sandbox-credentials`).
- To choose where it runs, its image and its size, configure the backend
  (:doc:`backends`).
- To keep a file the agent wrote, copy it to object storage before the sandbox is
  deleted with ``exports`` (:ref:`sandbox-results`).
- To keep the sandbox itself after the run, for a later task or a review, create it in
  a task and pass it to the agent with ``attach_to`` (:ref:`sandbox-attach`).

If the Dag already knows which commands to run, you don't need an agent: drive the
backend from a ``@task`` (:ref:`sandbox-results`).

.. _sandbox-configuring:

Control network access
----------------------

A sandbox has no outbound network by default, including DNS, and the model is told
so in the ``run_command`` tool description:

.. code-block:: text

    This sandbox has NO network access, including DNS, so installing packages or
    downloading anything will fail. Work with what the image already has.

To let it reach specific hosts, list them in ``allow_egress_to``. On Modal a hostname
list has to be accepted explicitly, for reasons below:

.. code-block:: python

    from airflow.providers.common.ai.sandbox import ModalSandboxBackend, SandboxSpec
    from airflow.providers.common.ai.toolsets import SandboxToolset

    SandboxToolset(
        ModalSandboxBackend(egress_enforcement="sni"),
        spec=SandboxSpec(allow_egress_to=["pypi.org", "files.pythonhosted.org"]),
    )

``allow_egress_to_cidrs`` lists IPv4 ranges instead. Both lists narrow the default
``block_network=True``, so don't combine them with ``block_network=False``, which
opens outbound access. Backends differ in what they can enforce:

.. list-table::
   :header-rows: 1
   :widths: 14 22 24 20 20

   * - Backend
     - Default (no network)
     - ``allow_egress_to``
     - ``allow_egress_to_cidrs``
     - ``block_network=False``
   * - Modal
     - Yes
     - With ``egress_enforcement="sni"``
     - Yes, any port
     - Yes; metadata endpoint and private ranges stay blocked
   * - ``sbx``
     - With ``host_network_policy="deny-all"``
     - With ``host_network_policy="deny-all"``
     - No
     - Yes; egress is whatever the host policy allows
   * - OpenSandbox
     - Yes, if the egress sidecar runs in ``dns+nft`` mode
     - Yes, same condition
     - No
     - Yes
   * - OpenShell
     - Yes
     - Yes, port 443 only
     - No
     - No
   * - Boat
     - No
     - No
     - No
     - Yes
   * - Islo
     - Yes
     - No
     - No
     - Yes

A setting the backend cannot enforce fails the task on the agent's first tool call,
when the toolset creates the sandbox, and the message says what to change. A bare
``SandboxSpec()`` on ``sbx``, where egress is a host-wide policy the backend cannot
see, fails without ``host_network_policy="deny-all"``:

.. code-block:: text

    SandboxTerminalError: SandboxSpec asks for no network egress, but this backend
    cannot enforce that per sandbox and the host policy has not been declared. Run
    'sbx policy init deny-all' on the worker host and pass
    host_network_policy='deny-all', or pass SandboxSpec(block_network=False) to
    acknowledge that egress is open.

Modal checks a hostname list against the name in the TLS handshake. That lets TLS on
port 443 through to listed hosts, but DNS stays open for every name, and a host that
shares a TLS endpoint with a listed one can be reached. Without
``egress_enforcement="sni"`` the backend refuses the list and says so:

.. code-block:: text

    SandboxTerminalError: SandboxSpec names a hostname allowlist, which Modal can only
    enforce by matching the TLS SNI: it allows TLS on port 443 to those hosts, but
    leaves DNS open for every hostname and cannot stop a host that shares a TLS
    endpoint with an allowed one from being reached. Pass
    ModalSandboxBackend(egress_enforcement='sni') to accept that, use
    allow_egress_to_cidrs for a destination with a fixed address, which Modal
    enforces at the address layer, or use SandboxSpec(block_network=True) with no
    allowlist, which Modal enforces exactly and which also blocks DNS.

What each backend enforces, and how, is under
:ref:`Modal <sandbox-backend-modal>`, :ref:`OpenSandbox <sandbox-backend-opensandbox>`,
:ref:`OpenShell <sandbox-backend-openshell>`, :ref:`Boat <sandbox-backend-boat>`,
:ref:`Islo <sandbox-backend-islo>` and :ref:`sbx <sandbox-backend-sbx>`.

Install packages
----------------

With no network, ``pip install`` inside the sandbox retries five times on
``Temporary failure in name resolution`` and then returns to the model:

.. code-block:: text

    [stderr]
    WARNING: Retrying (Retry(total=4, ...)) after connection broken by ...: Temporary failure in name resolution')': /simple/requests/
    ...
    ERROR: Could not find a version that satisfies the requirement requests (from versions: none)
    ERROR: No matching distribution found for requests
    [exit code: 1]

Install what the agent needs into the image instead:

.. code-block:: python

    import modal

    ModalSandboxBackend(
        image=modal.Image.from_registry("python:3.12-slim").pip_install("pandas", "pyarrow"),
    )

Then say in the agent's instructions which packages it has. The tool description
tells the model whether it has network access, but not what the image contains.

On a backend that supports hostname lists (see the table), you can allow ``pypi.org``
and ``files.pythonhosted.org`` instead, as in the example above; on Modal, only if the
hostname-list caveats are acceptable.

.. _sandbox-timeouts:

Size the sandbox and its timeouts
---------------------------------

Size the sandbox on the backend. A Modal sandbox without ``cpu`` requests 0.125 of a
CPU core, so set ``cpu`` for real work. Modal treats ``memory`` as a request: code in
the sandbox can allocate more. Other backends size differently; see
:ref:`sandbox-backend-differences`.

A command gets 60 seconds (``default_command_timeout``) unless the model asks for
more, and never more than ``max_command_timeout`` (300 seconds).

The sandbox itself is deleted when the backend's lifetime runs out: ``sandbox_timeout``
on Modal, 3600 seconds by default. The clock runs while the model thinks between tool
calls, so set it for the whole agent run.

Modal's ``idle_timeout`` is off by default. The model generating its next step looks
idle to Modal, and an idle timeout would delete the sandbox and the agent's files.

Give two sandboxes to one agent
-------------------------------

Tool names must be unique within an agent. Two ``SandboxToolset`` instances both
define ``run_command``, so the second needs a ``tool_prefix``:

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

Without it, the run fails on the first model request with:

.. code-block:: text

    UserError: SandboxToolset 'sandbox-sbx' defines a tool whose name conflicts with
    existing tool from SandboxToolset 'sandbox-sbx': 'run_command'. Rename the tool or
    wrap the toolset in a `PrefixedToolset` to avoid name conflicts.

Tell the model in its instructions which sandbox is for what.

.. _sandbox-credentials:

Give code in the sandbox a credential
-------------------------------------

Nothing from Airflow reaches the sandbox unless you put it in ``SandboxSpec.env``: no
connection, variable or worker environment variable. The credential that creates
the sandbox, such as your Modal token, stays in the task.

.. warning::

    The model can read anything in ``env``. Code it writes can print the environment,
    and tool results go into the model's context, from where they can reach the
    agent's output and any traces you record. Tool output is passed through Airflow's
    secret masker, which hides values Airflow knows are secrets, but not an encoded
    or altered copy. Give the sandbox a credential scoped to this one job, or none.

If the model only needs to query a system, not hold its key, use a
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` or
:class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`. They run the hook in
the task and return only the result.

``SandboxSpec`` is not templated, and it is built when the Dag file is parsed. To pass
a secret from a connection or a secrets backend, create the sandbox in a task, where
the lookup runs at run time, and attach the agent to it, which needs Modal
(:ref:`sandbox-attach`). Where you can, use the secret in that task to fetch the data,
write the data into the sandbox, and give the sandbox no secret at all.

Without an agent, the same task does the whole job. In outline, letting the sandbox
reach only the API's address:

.. code-block:: python

    @task
    def run_with_credential():
        from airflow.providers.common.ai.sandbox import ModalSandboxBackend, SandboxSpec
        from airflow.providers.common.compat.sdk import BaseHook

        token = BaseHook.get_connection("my_api").password
        backend = ModalSandboxBackend()
        sandbox = backend.create(
            spec=SandboxSpec(env={"API_TOKEN": token}, allow_egress_to_cidrs=["203.0.113.10/32"])
        )
        try:
            ...  # write_file, run_command, read_file
        finally:
            backend.destroy(sandbox)

Values must be strings; convert them with ``str()`` yourself. Modal refuses anything
else before creating the sandbox:

.. code-block:: text

    SandboxTerminalError: SandboxSpec.env must map strings to strings; these entries
    do not: ['PORT']. Convert the value at the Dag level, e.g. str(port), so what
    reaches the sandbox is what you meant.

.. _sandbox-results:

Get a file out
--------------

The agent's answer reaches XCom through ``output_type``, like any other agent output.
A file the agent builds cannot come back that way: ``read_file`` returns text, so a
parquet file or an image is corrupted, and the model sees at most 50 KiB of any file
(``max_output_bytes``).

Name the file in ``exports`` instead, with where it should go. When the run ends, the
toolset copies it out of the sandbox and then deletes the sandbox. The copy streams
through the worker, so neither the model's context nor XCom carries the file:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_sandbox_toolset.py
    :language: python
    :start-after: [START howto_sandbox_agent_export]
    :end-before: [END howto_sandbox_agent_export]

With ``ModalSandboxBackend()`` in place of ``sbx``, the ``normalize`` task log shows
the export, and ``load`` returns ``3``:

.. code-block:: text

    Exported staging.csv from sandbox sb-01M5ZKK8AJR1PG8Z6012AZW7S4 (172B)
    Published file:///tmp/airflow-sandbox-example/manual__2026-10-27T21:17:09.931899+00:00/staging.csv

The destination is templated, so each run writes its own file. It can be anything
:class:`~airflow.sdk.ObjectStoragePath` opens, with credentials from
``export_conn_id``. Files up to ``max_export_bytes`` (1 GiB) are copied.

Each file is first copied to a ``.partial`` name next to its destination. If a named
file is missing, is not a regular file, is over the limit, or cannot be copied, the
task fails and no destination changes. Only when every copy has succeeded are the
files moved into place; if a move fails, the task fails and the error lists the files
already in place. A run with no sandbox left at the end, because it never called a
tool or a timeout deleted the sandbox, fails with:

.. code-block:: text

    SandboxTerminalError: The run ended with no sandbox standing, so none of the files it was to export exist: 'staging.csv'.

A run that fails exports nothing, so a downstream task with
``trigger_rule=TriggerRule.ALL_DONE`` may find a file from an earlier try.

When the Dag already knows the job, skip the agent. A ``@task`` writes the input in
with ``write_file``, runs the command, and reads the result out with ``read_file``
under a byte limit you choose. The full Dag is ``example_sandbox_task_artifact`` in
``example_sandbox_toolset.py``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_sandbox_toolset.py
    :language: python
    :start-after: [START howto_sandbox_task_artifact]
    :end-before: [END howto_sandbox_task_artifact]

.. _sandbox-attach:

Share a sandbox between tasks
-----------------------------

A sandbox the toolset creates is deleted when the agent run ends. Create the sandbox
in a task instead, and pass its handle to the agent with ``attach_to``, when you need
to:

- keep the files after a failed run;
- let a second run, such as :ref:`HITL review <howto:hitl_review>`, see the first
  run's files;
- give the sandbox a secret from a connection.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_sandbox_toolset.py
    :language: python
    :start-after: [START howto_sandbox_attach]
    :end-before: [END howto_sandbox_attach]

``provision`` returns the sandbox's id, and the ``analyse`` task log shows the toolset
attaching to it with the time left:

.. code-block:: text

    Attached to sandbox sb-01M5ZKXF2HPVDBYY77EFZVQZBG on backend modal as example_sandbox_attach/manual__2026-10-27T21:22:55.304167+00:00/analyse; 1795s of its lifetime remain

``provision`` creates the sandbox, stamps it with its Dag run as the owner, and writes
the input in. ``analyse`` attaches, and the toolset leaves the sandbox running when
the run ends. ``collect`` runs whatever happened to the agent, reads the report out
and deletes the sandbox.

Only Modal supports this. On any other backend, ``attach_to`` fails when the Dag is
parsed:

.. code-block:: text

    ValueError: attach_to needs a backend that can find a sandbox from another task,
    and 'sbx' cannot: it is not an AttachableSandboxBackend. Drop attach_to and let the
    toolset provision its own sandbox, or provision on a backend that can be attached
    to, such as ModalSandboxBackend.

The toolset attaches only to a sandbox whose owner matches. By default that is the
Dag run the agent task is in, which is what ``dag_run_owner(context)`` returns in the
provisioning task. Pass ``owner=`` to the toolset if you provisioned under a different
name. Anyone holding the same Modal credential can still change the owner tag or use
the sandbox directly, so this catches a wrong handle, not a hostile Dag author.

While an agent run is attached, other tasks, other Dag runs and other map indexes are
refused; the same task retrying is let in, so a retry finds the earlier attempt's
files. The check writes a holder tag and reads it back, which is not a lock, so give
each task that needs its own workspace its own sandbox rather than relying on it.

The sandbox lives for the provisioning backend's ``sandbox_timeout``, counted from
``provision`` through every gap between tasks and any review wait. Leave
``idle_timeout`` unset there; the backend refuses an owner when it is set:

.. code-block:: text

    SandboxTerminalError: SandboxSpec names an owner, so another task will attach to
    this sandbox later, but this backend has idle_timeout=300, which would reclaim it
    while it waits between tasks. Leave idle_timeout unset on the backend that
    provisions a sandbox for attaching, and size sandbox_timeout to cover the wait.

The toolset never deletes an attached sandbox, so give the task that does a
``trigger_rule=TriggerRule.ALL_DONE``. If it reads the file and then fails to delete
the sandbox, let it succeed: ``sandbox_timeout`` cleans up.

.. _sandbox-lifecycle:

What happens when something fails
---------------------------------

The toolset creates the sandbox on the first tool call and deletes it when the run
ends. Files persist between calls in the same run; shell variables do not, because
each ``run_command`` starts a new shell.

A command that fails goes back to the model, which can fix it. This is what the model
saw on Modal for a command that printed a line and exited with 3, then one that ran
past a two-second timeout, then a ``cat`` of a file written before both:

.. code-block:: text

    [stdout]
    partial
    [exit code: 3]

    (no output)
    [timed out after 2s]

    [stdout]
    kept across calls

The sandbox and its files survive the timeout. A backend that can only stop a command
by deleting the sandbox tells the model its earlier files are gone, and the next call
gets a new sandbox.

Errors in the sandbox, such as a missing path, a permission error or a file over the
read limit, go back to the model as a retry prompt. Errors the model cannot fix fail
the task, and Airflow's task retries take over: rejected credentials, a sandbox that
has passed its lifetime mid-run, and any error while creating the sandbox.

Retries, durable runs and review
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the toolset creates its own sandbox, a task retry gets a new, empty one, and an
attached sandbox still has the earlier attempt's files. ``AgentOperator`` refuses the
two features
that would assume otherwise, when the Dag is parsed. It cannot see a toolset built per
run from a callable, so check that case yourself. ``durable=True`` is refused with any
``SandboxToolset``:

.. code-block:: text

    ValueError: durable=True cannot be used with a SandboxToolset: cached tool results
    would be replayed without touching the sandbox, so the workspace would not match
    the transcript. Drop durable=True, or move the sandbox work into its own task.

``enable_hitl_review=True`` is refused unless every ``SandboxToolset`` is attached to a
sandbox another task created:

.. code-block:: text

    ValueError: enable_hitl_review=True cannot be used with a SandboxToolset that
    provisions its own sandbox: a regenerated run would start from an empty sandbox
    while its history describes files from the first run. Attach the toolset to a
    sandbox another task provisioned (attach_to=...), drop enable_hitl_review=True, or
    move the sandbox work into its own task.

With an attached sandbox, the reviewer has to answer before ``sandbox_timeout``, so
set ``hitl_timeout`` below what will be left. :doc:`Per-tool approval
</tool_approval>` works the same way: allowed with an attached sandbox, and a tool
that needs approval fails the task when the toolset created its own sandbox.

.. _sandbox-cost:

Clean up and control cost
-------------------------

A sandbox is billed from creation until it is deleted, including the time the model
spends thinking. ``usage_limits`` does not cap sandbox cost, because ``cost_limit``
counts only model calls. The backend's lifetime setting, ``sandbox_timeout`` on Modal,
is the most one sandbox can bill for.

If the worker is killed, the toolset never deletes the sandbox, and the backend's
lifetime is what cleans up. ``sbx`` and OpenShell have no lifetime, so their sandboxes
stay until someone removes them. A delete that fails is logged with the sandbox name
and does not fail the task.

On Modal, ``destroy`` asks Modal to stop the sandbox and returns without waiting.
Every sandbox carries an ``airflow_sandbox`` tag plus the ``tags`` you give the
backend, so with ``ModalSandboxBackend(tags={"dag_id": "my_dag"})`` this lists what
that Dag left:

.. code-block:: python

    modal.Sandbox.list(app_id=..., tags={"dag_id": "my_dag"})
