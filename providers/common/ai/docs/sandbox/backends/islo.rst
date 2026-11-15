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

.. _sandbox-backend-islo:

Islo (hosted microVM)
=====================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.sandbox.islo.IsloSandboxBackend` runs each
sandbox in an `islo.dev <https://islo.dev>`__ microVM, provisioned over the API.
Like Modal and unlike ``sbx``, the worker only speaks HTTP: it needs no local
daemon and no host virtualization, so it runs from a containerized worker and on
Kubernetes.

Install the SDK extra:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[islo]"

.. code-block:: python

    from airflow.providers.common.ai.sandbox import IsloSandboxBackend
    from airflow.providers.common.ai.toolsets import SandboxToolset

    SandboxToolset(IsloSandboxBackend())

Credentials are ambient. On first use the SDK reads ``ISLO_API_KEY``, and optionally
``ISLO_BASE_URL`` and ``ISLO_COMPUTE_URL``, from the worker environment. Modal's
``modal`` connection comes from the Modal provider; an ``islo`` connection type waits
for an Islo provider.

Constructor parameters:

- ``image``, ``vcpus``, ``memory_mb``: image and sizing. ``None`` (default) uses
  the server default for each. The server default image is Debian based, and any
  Debian or Ubuntu based image, including ``python:*-slim``, has the GNU ``find``,
  ``stat`` and ``tail`` the file operations need; Alpine and other busybox images
  do not.
- ``pause_after_idle``: Seconds without a command or file operation after which
  the server pauses the microVM and stops charging for its compute. Default
  ``600``; ``None`` disables it.
- ``auto_resume``: ``"on_activity"`` (default) resumes a paused sandbox on the
  next tool call, so a long think between calls costs a resume rather than the
  run; ``"never"`` leaves it paused, and the backend then treats a paused sandbox
  as unusable.
- ``delete_after``: Seconds after **creation** at which the server deletes the
  sandbox, whether or not it is in use. Default ``86400``; ``None`` disables it.
  This is the backstop for a worker killed mid-run; it is not renewed while the
  sandbox is busy, so keep it longer than the longest run you expect.

``SandboxSpec.env`` becomes the process environment of every command, and
``block_network`` maps to the API's ``internet_enabled``. Four things Islo cannot
do are refused at ``create`` rather than silently dropped: ``allow_egress_to`` and
``allow_egress_to_cidrs`` (the API turns outbound access on or off, with no
per-host or per-address rule), a ``PATH`` entry in ``env`` (the runner sets
``PATH`` for every command itself), and ``owner`` (this backend keeps no
per-sandbox metadata, so it cannot be attached to from another task).

File reads, writes and exports move file contents through Islo's native
streaming APIs. Everything else runs through a shell wrapper in the sandbox, so
the image needs ``sh``, ``mkdir`` and ``rm`` for the wrapper, ``tail`` to bound
command output, ``dirname`` to create a written file's parent directory,
``stat`` to size an over-budget read and to check a file before it is exported,
``realpath`` to resolve a relative export path (the download API takes absolute
paths only), and GNU ``find`` for directory listings. The compute API caps each output stream
at 1 MiB and keeps the tail. The backend captures each stream to a scratch file in
the sandbox and returns only its last ``max_output_bytes`` -- where a traceback
and the exit status live -- so the worker sees a window sized to the caller's
budget, a budget above the server cap is clamped to it, and total output is
bounded by the sandbox's own ephemeral disk rather than by worker memory.

The backend enforces the command deadline itself, because the API's
``timeout_secs`` is accepted but not enforced, and times it from the API's answer
to the start request, so a slow start does not use it up. Polling rides out
transient API errors until the deadline. If the command is still running when
the deadline passes, the backend deletes the microVM and reports the command as
timed out with ``sandbox_terminated`` set, so the toolset provisions a fresh
sandbox for the next call. A deletion the API refused is logged rather than
failing the task, and ``delete_after`` reclaims the microVM later; with
``delete_after=None`` nothing does, and the warning says so. If the API was still
failing at the deadline, nothing is known about the command, so the task fails
instead and Airflow's retry takes over.

A missing file, a directory passed as a file, a relative path, and a write onto a
directory or under a pseudo-filesystem such as ``/proc`` or ``/sys`` come back to
the model as a recoverable error it can correct, once the backend has confirmed
the sandbox itself is still usable.
