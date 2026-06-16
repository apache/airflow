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

Creating a new Language SDK
===========================

Starting from 3.3, the standard Airflow workers can run task code implemented in
languages other than Python, using a foreign Language SDK. This document
describes how a new Language SDK can be contributed, so Airflow can execute
tasks implemented in the language.

Two components are needed for Airflow to understand how to execute such a task:

* A **coordinator implemented in Python** for Airflow to understand how to wire
  up the foreign package.
* A **language SDK implemented in the target language** to talk to the
  coordinator.

The two components are largely independent: the coordinator decides *how* to
start the foreign runtime and *how* to communicate with it, and the language SDK
implements the other end of whatever protocol the coordinator chooses. There is
no single mandated communication mechanism.

.. contents:: Table of Contents
   :depth: 2
   :local:


Python Coordinator
------------------

The coordinator is a Python class that Airflow calls when a stub task with a
matching queue is triggered. Its sole required method is ``execute_task``, which
must start the foreign runtime, hand off the task, and return the final task
state. The base class is
:class:`airflow.sdk.execution_time.coordinator.BaseCoordinator`.

A coordinator can communicate with the foreign runtime in any way it chooses.
This can be a subprocess over TCP sockets, a gRPC server, shared memory, a
message queue, or anything else. The choice of transport is entirely up to the
coordinator and its SDK counterpart.

Choosing an implementation path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In practice, almost all coordinators will fall into one of the following
categories. Choose the one that fits the target runtime:

Use :class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator`
    If the language SDK compiles to a self-contained native executable, the
    existing ``ExecutableCoordinator`` can discover and launch it with zero
    Python code. The executable just needs to carry an ``AFBNDL01`` metadata
    trailer (see `Native Executable Bundle Format`_ below). This is the simplest
    path since you would not need to write any Python code.

Subclass :class:`~airflow.sdk.coordinators._subprocess.SubprocessCoordinator`
    If a command is needed (e.g. `java` for JRE, `node` for Node), but
    the produced runtime can communicate over TCP, consider subclassing
    ``SubprocessCoordinator`` and implement ``_build_execute_task_command``.
    The base class handles all TCP socket lifecycle, process ownership
    verification, startup draining, and teardown.

Subclass ``BaseCoordinator`` directly
    For runtimes that use a completely different transport (gRPC, shared memory,
    a persistent daemon, etc.), subclass ``BaseCoordinator`` and implement
    ``execute_task`` from scratch.

SubprocessCoordinator: implementing ``_build_execute_task_command``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`~airflow.sdk.coordinators._subprocess.SubprocessCoordinator` handles
the full subprocess lifecycle. When a task is triggered, it:

1. Binds two ephemeral TCP server sockets on ``127.0.0.1``.
2. Spawns the subprocess with ``--comm=<host>:<port>`` and
   ``--logs=<host>:<port>`` appended to the command.
3. Waits for the subprocess to connect to both sockets.
4. Signal to subprocess to begin executing user code.
5. Forwards task log lines received over ``--logs`` to the Airflow log
   infrastructure.
6. Tears everything down when the subprocess exits or the startup times out.

The ``--comm`` socket is the bidirectional task execution channel described in
`Language SDK (target language)`_. The ``--logs`` socket is used to forward
*infrastructure logs* (messages emitted by the SDK, not user code) to be merged
into Airflow worker logs.

Subclasses only need to supply the command to run and the wire-schema version
the subprocess understands:

.. code-block:: python

    def _build_execute_task_command(self, *, what: TaskInstanceDTO) -> tuple[list[str], str]: ...

The method returns a ``(command, subprocess_schema_version)`` pair:

* ``command`` — the subprocess argv list. Do **not** include ``--comm`` or
  ``--logs``; the base class appends those flags after binding the sockets.
* ``subprocess_schema_version`` — the ``YYYY-MM-DD`` wire-schema version the
  subprocess understands, used by the supervisor to negotiate message formats
  across SDK versions. See `Supervisor Schema`_ below.

Supervisor Schema
~~~~~~~~~~~~~~~~~

The Supervisor Schema is the formal contract between the supervisor and a
lang-SDK subprocess. It is generated from Pydantic models defined in the
supervisor and published as a JSON Schema file at
``task-sdk/src/airflow/sdk/execution_time/schema/schema.json``. The file
describes every message type that can appear on the comm socket in either
direction, and carries an ``api_version`` field with a ``YYYY-MM-DD`` date
string that identifies the schema revision.

The schema evolves over time, so the SDK must tell the supervisor what API
version it was built against for this bridging to work. How the target-language
SDK uses ``schema.json`` to generate or validate its message types is covered in
`Language SDK`_ below.

Adding a new coordinator class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you decide to implement a new coordinator, place it like this:

.. code-block:: text

    task-sdk/src/airflow/sdk/coordinators/
        <language>/
            __init__.py
            coordinator.py

Add unit tests under ``task-sdk/tests/`` following the same structure, and
integration tests in ``task-sdk/tests/integration/``.


Native Executable Bundle Format
-------------------------------

If the target runtime compiles to a self-contained native executable, the
:class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator` can
discover and launch it automatically. For the coordinator to understand the
bundle correctly, extra metadata should be appended to the executable by a
custom bundling step at build-time.

See :ref:`Executable Bundle Spec` in Task SDK documentation for details.


Language SDK (target language)
-------------------------------

This section describes what the SDK in the target language must implement when
the coordinator uses the subprocess + TCP socket transport (i.e. when the
coordinator is a subclass of ``SubprocessCoordinator`` (this includes
``ExecutableCoordinator``). If a custom coordinator with a different transport
is used, the protocol between the coordinator and the SDK is entirely up to the
implementer.

Startup
~~~~~~~

The supervisor launches the subprocess with two command line arguments:

.. code-block:: text

    --comm=<host>:<port>
    --logs=<host>:<port>

The SDK process MUST be able to parse the arguments, and connect to both sockets
as soon as possible. The supervisor verifies that the connecting peer belongs
to the launched process tree, so the SDK MUST connect from the same process or
one of its descendants.

Once both connections are accepted, the supervisor sends a ``StartupDetails``
message on the comm socket to initiate execution.

Wire protocol
~~~~~~~~~~~~~

All communication on the ``--comm`` socket uses **length-prefixed MessagePack
frames**:

.. code-block:: text

    [4-byte big-endian uint32: payload length][payload bytes]

The payload is a MessagePack-encoded array. Two shapes are used:

* SDK → Supervisor uses a 2-element array ``[id, body]``.

  * ``id`` is an integer that identifies this request uniquely within the
    connection. Responses echo the same ``id`` for correlation.
  * ``body`` is the payload.

* Supervisor → SDK uses a 3-element array ``[id, body, error]``.

  * ``id`` mirrors the request ``id`` if this message is a response to a request
    initiated by the SDK .
  * ``body`` is the payload, or ``null`` on error.
  * ``error`` — an ``ErrorResponse`` map on failure, or ``null`` on success.

In both cases, the payload is a MessagePack map with a ``"type"`` key naming the
message. Maximum payload size is ``2³² - 1`` bytes. Sending a larger payload is
an error.

Message types
~~~~~~~~~~~~~

The complete field-level definition of every message type — names, types, and
constraints — lives in the Supervisor Schema file:

.. code-block:: text

    task-sdk/src/airflow/sdk/execution_time/schema/schema.json

An SDK implementation can generate message types for the target language
directly from it with a code generator that supports JSON schema input.

Request/response correlation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each outbound request sent by the SDK carries a monotonically increasing integer
``id``. The supervisor echoes this ``id`` in the response frame so the SDK can
match responses to requests. The SDK MUST correlate by ``id`` whenever multiple
requests can be outstanding simultaneously (e.g. when tasks are executed
concurrently, or when a single task issues multiple requests).

Error handling
~~~~~~~~~~~~~~

* If the task raises an unhandled error, the SDK MUST send a ``TaskState``
  message with ``"state": "failed"`` before closing the comm socket.
* If the task fails but still has retries left, the SDK MUST send a
  ``RetryTask`` message instead so the supervisor moves the task to
  ``up_for_retry``. Field names MUST match the Supervisor Schema exactly — the
  failure detail key is ``retry_reason``, not ``reason``.
* If the process exits without sending a terminal message, the supervisor marks
  the task instance ``failed`` based on the abnormal exit, but the task log may
  be incomplete.
* If the supervisor returns an ``ErrorResponse`` to a mid-task request, the SDK
  SHOULD propagate it as an error to the task function.


Testing
-------

A language SDK implementation should include:

* **Unit tests** for the framing layer (encode/decode round-trips, oversized
  frames, corrupt length prefixes).
* **Unit tests** for each message type in both directions.
* **Integration tests** against a live supervisor using Breeze.
