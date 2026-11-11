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

.. _sandbox-backend-boat:

Boat (hosted)
=============

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.sandbox.boat.BoatSandboxBackend` runs each
sandbox on `Boat <https://docs.boat.dev/quickstart>`__, the hosted cloud-computer
API formerly called Ascii Box. The Airflow worker needs only network access and
an API key: no local daemon, Docker socket, KVM or nested virtualization, so it
runs from a containerized worker and on Kubernetes.

Install the SDK extra:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[boat]"

.. code-block:: python

    from airflow.providers.common.ai.sandbox import BoatSandboxBackend, SandboxSpec
    from airflow.providers.common.ai.toolsets import SandboxToolset

    SandboxToolset(
        BoatSandboxBackend(),
        spec=SandboxSpec(block_network=False),
    )

Credentials can come from a generic Airflow connection, resolved lazily on first
use:

.. code-block:: python

    SandboxToolset(
        BoatSandboxBackend(boat_conn_id="boat_default"),
        spec=SandboxSpec(block_network=False),
    )

The connection ``password`` is the Boat API key and is required. ``host``, when
set, is the full API base URL, such as ``https://boat.dev/api/v1``, which is also
the default. It must start with ``https://`` or ``http://``: a bare domain such as
``boat.dev``, which is what Airflow's connection URI form leaves in ``host``, is
refused, since the API key would otherwise be sent to it over plain HTTP. With
``boat_conn_id=None``, the default, the backend reads ``BOAT_API_KEY`` (required)
and optional ``BOAT_BASE_URL`` from the worker environment instead, and
``BOAT_BASE_URL`` must carry the scheme too.

Constructor parameters:

- ``org``: the Boat wallet each sandbox is billed to, by organization id or name,
  or ``"personal"``. Default ``"personal"``, which bills your own account whatever
  its active wallet is. See the warning below before naming an organization.
- ``machine_type``: ``"small"``, ``"default"`` or ``"large"``.
- ``ttl_seconds``: server-side TTL after which Boat stops the sandbox, a whole
  number of seconds up to ``2592000`` (30 days). Default ``3600``.
- ``ready_timeout``: provisioning deadline in seconds, from the create request
  until the sandbox is ready. Default ``300``.
- ``request_timeout``: HTTP timeout in seconds for an API call that answers at
  once, such as the create request, a status check or a delete, and the time
  added to the operation's own for a call that waits on one: a command's
  deadline, or 120 seconds for a file write. Default ``30``.

A create request that times out, fails in transport, or gets a server error may
still have started a sandbox, so it is sent again with the same idempotency key,
and Boat answers with the sandbox the first request created rather than starting
a second one. While Boat is still creating that sandbox, it answers the resent
request with 409 ``idempotency_in_progress``, and the backend sends it again two
seconds after each such answer until ``ready_timeout`` runs out. Each resend gets
what is left of ``ready_timeout`` plus ``request_timeout``, so provisioning takes
at most their sum, and a sandbox a resend finds too late to become ready in time
is deleted rather than left running until its TTL. If a resend fails any other
way, or Boat is still creating the sandbox when ``ready_timeout`` runs out, the
backend never receives the sandbox's id, so any sandbox the first request started
keeps a server-assigned name and runs until Boat stops it at ``ttl_seconds``.

Every sandbox is created with Boat's ``noEnv`` flag, so it gets none of your Boat
account's stored environment variables, secret files or credentials, and cannot
act on your account or your other sandboxes.

.. warning::

    Boat gives every member of an organization use of a ``noEnv`` sandbox billed
    to that organization: they can open it and run commands in it, so they can
    read and write anything in it, including the ``SandboxSpec.env`` values each
    command receives. Pass ``org`` only when everyone in that organization may see
    what the agent works on.

``SandboxSpec.env`` is passed when the sandbox is created and also exported at
the start of every command, so each value is sent again with every command
request. **Boat cannot enforce a deny-all network policy, a per-domain egress
allowlist, or a CIDR egress allowlist**, so the backend refuses
``block_network=True``, ``allow_egress_to``, and ``allow_egress_to_cidrs``
rather than silently provisioning something weaker than the spec asked for.
Since ``block_network`` defaults to ``True``, that includes a bare
``SandboxSpec()``: pass ``SandboxSpec(block_network=False)`` to state that open
egress is acceptable, or use Modal when it is not.

Writes use Boat's native file API, which accepts only paths that resolve under
``/home/user`` (where relative paths land) or ``/tmp``. The parent directory is
created first with ``mkdir -p`` in the guest, so a ``write_file`` anywhere else
fails with ``mkdir``'s own error when the guest user cannot create the parent
directory, and with Boat's ``invalid_path`` error when that directory exists or
could be created. Either way the model gets it back as a tool error it can
retry. Reads deliberately keep the inherited bounded shell implementation, so
``max_bytes`` is enforced inside the guest before file contents reach worker
memory.

That path rule binds only ``write_file``; it is not a jail. Commands run as the
guest user ``user``, which `Boat documents <https://docs.boat.dev/machines>`__ as
having root through ``sudo`` with no password, so a command the model writes can
read and write anywhere in the sandbox filesystem. The sandbox also has a public
IPv4 or IPv6 address, and its firewall is inside the guest: Boat's
`hosting guide <https://docs.boat.dev/hosting>`__ opens a port to the internet
with ``ufw``, which a command can do too. The sandbox boundary is what contains
that.

A command's deadline is enforced inside the sandbox by GNU coreutils
``timeout``, which sends ``SIGTERM`` and then ``SIGKILL`` five seconds later, so
a command that runs out of time returns the tail of what it printed, and the
sandbox and its files survive. Boat's own deadline is set 15 seconds past that
one, and Boat's API caps it at 600 seconds, so any command that asks for more
than 585 seconds gets 585, and the result reports that deadline to the model.
Only when Boat's own deadline ends the call, because the one in the guest did
not, is the sandbox torn down, as is a sandbox that never becomes ready. A file
read, listing, or write that Boat's deadline ends that way fails the task, since
there is no sandbox left for the model to retry against. If the worker dies
first, the server-side TTL stops the sandbox. Sandboxes are created without
snapshots, so stopping one erases its disk rather than keeping it for an
operator to remove, and with monitoring off, so Boat keeps no history of the
commands, their output, or the environment variables exported into them.
