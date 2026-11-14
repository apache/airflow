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

.. _sandbox-backend-boxd:

boxd (hosted microVM)
=====================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.sandbox.boxd.BoxdSandboxBackend` runs each
sandbox in a `boxd <https://boxd.sh/?ref=airflow>`__ machine: a KVM microVM with
its own kernel and disk, provisioned over the API in about a second. The worker
only speaks to the API, with no local daemon or host virtualization, so it runs
from a containerized worker and on Kubernetes.

Install the SDK extra:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[boxd]"

.. code-block:: python

    from airflow.providers.common.ai.sandbox import BoxdSandboxBackend
    from airflow.providers.common.ai.toolsets import SandboxToolset

    SandboxToolset(BoxdSandboxBackend())

Credentials can come from a generic Airflow connection, resolved lazily on first
use, whose ``password`` is the boxd API key:

.. code-block:: python

    SandboxToolset(BoxdSandboxBackend(boxd_conn_id="boxd_default"))

With ``boxd_conn_id=None``, the default, the backend reads ``BOXD_API_KEY`` from
the worker environment instead. A key is required either way: the boxd SDK can
also sign in as the boxd machine it runs on, and a worker that happens to run on
one must not act as that machine's owner unless told to. The SDK also reads
``BOXD_REGION`` (boxd 0.3.0 and later) and ``BOXD_BASE_URL`` from the worker
environment, to pick a region or another boxd cluster.

Constructor parameters:

- ``size``: machine size as ``"<vcpu>x<memory GiB>"``: ``"1x4"``, ``"2x8"``,
  ``"4x16"`` or ``"8x32"``. ``None`` (default) uses the organization's default size.
- ``snapshot``: a boxd snapshot to boot each sandbox from, for an image with your
  packages already installed. ``None`` (default) boots the default image, Ubuntu
  24.04 with Python, Node.js and the GNU tools the file operations need.
- ``delete_after``: seconds after **creation** at which boxd deletes the machine,
  whether or not it is in use. Default ``3600``; ``None`` disables it. This is the
  backstop for a worker killed mid-run, and it keeps counting while the machine
  sleeps, so keep it longer than the longest run you expect.
- ``name_prefix``: the start of each machine's name, followed by a random suffix.
  Default ``"airflow-sandbox"``.
- ``labels``: extra labels for every machine, such as ``{"team": "data"}``, so you
  can find them with ``boxd machine list --label team=data``.
- ``ready_timeout``: seconds from the create request until the machine answers a
  command. Default ``120``.

Every sandbox is an **isolated** boxd machine. It has no metadata endpoint, no
``boxd`` CLI inside and none of the account's integration logins, so code in it
cannot act on your boxd account or reach your other machines over the
organization's private network. Commands run as the machine's user ``boxd``,
who has ``sudo`` without a password; the isolation and the egress allowlist are
enforced outside the guest, so root inside the machine does not lift them.

``SandboxSpec.env`` becomes part of the machine spec, so every command gets it.
The network policy becomes the machine's egress allowlist, set in the same create
request, before anything runs in the machine:

- ``block_network=True`` with no allowlist is the allowlist ``none``: nothing is
  reachable, and DNS lookups are refused.
- ``allow_egress_to`` is a list of hostnames, wildcards such as ``*.github.com``
  included. boxd admits HTTP and HTTPS to a listed host by name, so plain HTTP to
  it works too, although the model is told HTTPS only. boxd documents that other
  protocols reach the addresses the machine resolved for a listed name as well.
- ``allow_egress_to_cidrs`` takes public IPv4 addresses and ranges, and admits
  any protocol to them except HTTP and HTTPS, which boxd admits by hostname only.
  To call a web API at a fixed address, list its hostname in ``allow_egress_to``.
  A private range such as ``10.0.0.0/8`` is refused when the machine is created.
- ``block_network=False`` leaves egress open. An allowlist next to it is refused,
  as on Modal.
- Hosts that a boxd secret the machine holds is bound to are always reachable.

A command runs through ``bash`` in ``/home/boxd``, under GNU coreutils ``timeout``,
which sends ``SIGTERM`` at the deadline and ``SIGKILL`` five seconds later. Its
output goes to scratch files in the machine and only the last ``max_output_bytes``
of each stream come back, so a timed-out command returns the tail of what it
printed, and the machine and its files survive. The API call itself is given up
30 seconds after that; only then, when the deadline inside the guest did not end
the command, is the machine deleted and reported as ``sandbox_terminated``.

``write_file`` uses boxd's file API, which creates missing parent directories,
replaces a file in one step and writes through a symlink to its target. A
permission error or a path through a regular file comes back to the model as a
recoverable error. Reads, listings and exports use the inherited shell
implementations, so ``max_bytes`` is enforced inside the guest and every
transfer is bounded by a command deadline. Relative paths resolve against
``/home/boxd`` for every tool.

A boxd sandbox can be attached to from another task (:ref:`sandbox-attach`). The
ownership tags are machine labels, which any process holding an API key of the
organization can read and change. A label holds at most 256 characters, so an
owner or holder longer than that is refused, and a network policy longer than
that is not recorded, in which case the attaching agent is told to test what it
can reach. ``delete_after`` is the lifetime an attaching toolset is told about. A
machine left idle between tasks may go to standby or hibernate, depending on the
organization's settings, and wakes on the next command.
