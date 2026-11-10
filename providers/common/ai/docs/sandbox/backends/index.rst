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

Sandbox backends
================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

.. toctree::
    :maxdepth: 1

    modal
    islo
    boat
    opensandbox
    openshell
    sbx
    custom

.. _sandbox-backend-differences:

What differs between the backends
---------------------------------

Swapping the backend is one constructor argument, and tool names, spec and prompt
do not change. Four behaviours do, so read them before assuming the same Dag
behaves identically everywhere:

- **CPU.** ``sbx`` gives a sandbox every host CPU; Modal defaults to a request of
  0.125 of one, so set ``cpu``; OpenSandbox and OpenShell take ``cpu`` as a limit the
  server enforces; Boat sizes by ``machine_type``; Islo uses the server default unless
  you set ``vcpus``.
- **Egress allowlists.** ``sbx`` enforces ``allow_egress_to`` at the host policy
  layer; Modal matches TLS handshake names, which is weaker and has to be opted
  into; OpenSandbox enforces it in an egress sidecar, and the backend reads the
  enforced policy and the sidecar's mode back rather than trusting the create
  request, refusing a sidecar that filters only name resolution (``dns``) rather
  than connections (``dns+nft``); OpenShell enforces it in a per-sandbox supervisor
  on port 443 only, and reads the effective policy back at create and around every
  operation. ``allow_egress_to_cidrs`` is enforced at the address layer on Modal and
  refused on ``sbx``, OpenSandbox, OpenShell and Islo; OpenShell's address rules admit
  TCP on listed ports only, and it also refuses ``block_network=False``. Islo has no
  per-host form and refuses the spec; Boat has no form of egress control at all and
  refuses every network restriction. Both refuse rather than provision something
  weaker.
- **Command timeouts.** A timeout destroys an ``sbx`` or Islo sandbox and its files;
  Modal, Boat and a server-enforced OpenSandbox timeout preserve the sandbox and
  files. OpenSandbox destroys it only if the command event stream itself stalls
  past the client-side grace period, and Boat only if its own deadline, set past
  the one in the guest, ends the call. OpenShell kills the command's session and
  keeps the sandbox; if the gateway stops relaying the command, the backend asks it
  to delete the sandbox, and only a delete that fails too leaves it for the reaper
  described under :ref:`OpenShell <sandbox-backend-openshell>`.
- **Symlinks.** ``write_file`` through a symlink follows the link on ``sbx``, Boat
  and OpenShell, which write the target and leave the link in place, and replaces
  the link with a regular file on Modal and OpenSandbox.
- **Attaching.** A Modal sandbox can be provisioned by one task and used by an
  agent in another (:ref:`sandbox-attach`). An ``sbx`` microVM lives on the worker
  that created it and cannot be reached from another task, OpenSandbox, Boat and
  Islo have no per-sandbox metadata the ownership rules could be kept in, and
  OpenShell does not implement attaching yet, so all five refuse ``SandboxSpec.owner``
  and the toolset refuses ``attach_to`` for them.
