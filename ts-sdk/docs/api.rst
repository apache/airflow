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

TypeScript SDK API Reference
============================

This page documents the full public API of the Apache Airflow TypeScript SDK
(the package root and the ``./coordinator`` entry points). Anything not listed
here should be assumed to be internal and is not part of the public API.

The reference is generated from the TypeScript sources with
`sphinx-js <https://github.com/pyodide/sphinx-js>`__ via TypeDoc.

Task Handlers
-------------

A task handler is a function registered against a ``(dagId, taskId)`` pair. It
receives the runtime context and a client for interacting with Airflow.

.. js:autofunction:: registerTask

.. js:autofunction:: listRegisteredTasks

.. js:autoattribute:: TaskHandler

.. js:autoclass:: TaskContext
   :members:

.. js:autoclass:: TaskHandlerArgs
   :members:

.. js:autoclass:: TaskRegistration
   :members:

Task Client
-----------

The ``TaskClient`` is the runtime interface for reading and writing
task-time data (Variables, XComs, and Connections).

.. js:autoclass:: TaskClient
   :members:

.. js:autoclass:: VariableNotFoundError
   :members:

.. js:autoclass:: ConnectionResult
   :members:

.. js:autoclass:: GetXComOpts
   :members:

.. js:autoclass:: SetXComOpts
   :members:

.. js:autoattribute:: JsonValue

Coordinator
-----------

The coordinator runtime bridges the Airflow worker and the Node.js process that
runs registered TypeScript handlers. These are also available from the
``./coordinator`` entry point.

.. js:autofunction:: startCoordinator

.. js:autoclass:: StartCoordinatorOptions
   :members:

.. js:autoattribute:: SUPERVISOR_API_VERSION
