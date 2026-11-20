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

.. _sandbox-byo:

Bringing your own backend
=========================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

Any vendor that can create a sandbox, run a command in it and destroy it can plug
in. Subclass :class:`~airflow.providers.common.ai.sandbox.SandboxBackend` in your
own package and pass an instance to ``SandboxToolset``.

**Three methods are required**: ``create``, ``run_command`` and ``destroy``. The
file operations ship as defaults implemented over ``run_command``, because
reading, writing, listing and exporting a file are all expressible as shell
commands. The default ``export_file``, behind ``SandboxToolset(exports=...)``,
copies a file in 4 MiB slices, one command each, and needs ``stat``, ``tail``,
``head`` and ``base64`` in the guest. It relies on ``run_command`` returning each
slice's output intact, or setting ``stdout_truncated`` when it could not. Override
it when the vendor can stream a download, as the ``sbx``, OpenSandbox and Islo
backends do. Override the others only when the vendor has a native file API:

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

**If your sandboxes can be found again from another process**, subclass
:class:`~airflow.providers.common.ai.sandbox.AttachableSandboxBackend` instead,
and a ``@task`` can provision a sandbox for an agent task to attach to
(:ref:`sandbox-attach`). It adds two methods, ``read_tags`` and ``write_tags``,
over whatever key-value metadata the vendor keeps on a sandbox, and the ownership
rules are written once on the base class on top of them. Three things the base
class relies on: ``read_tags`` raises ``SandboxTerminalError`` for a sandbox that
does not exist or has ended; ``write_tags`` replaces the whole set, because
releasing a claim is a rewrite without the holder key; and ``create`` stamps
``SandboxSpec.owner`` under ``OWNER_TAG``, the sandbox's end time as Unix seconds
under ``EXPIRES_AT_TAG``, and the network policy under ``NETWORK_TAG`` using
``encode_network_policy(spec)``, all importable from
``airflow.providers.common.ai.sandbox.base``. If the vendor lets a caller of your
backend set metadata too, make your reserved keys overwrite theirs. Stamping the
working directory under ``WORKDIR_TAG`` is optional; without it the attaching
backend asks the sandbox. The toolset shortens ``run_command`` to the remaining
lifetime itself; a backend clamps only if its own file operations need it.
