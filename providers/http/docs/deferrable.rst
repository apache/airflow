
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

.. _howto/deferrable:HttpOperator:

Deferrable HttpOperator
=======================

:class:`~airflow.providers.http.operators.http.HttpOperator` can run in deferrable mode
(``deferrable=True``, or via ``[operators] default_deferrable``). In that mode the operator
defers immediately and the HTTP request is executed in the Triggerer by
:class:`~airflow.providers.http.triggers.http.HttpTrigger`.

Triggerer restart can replay the request
----------------------------------------

The Triggerer persists trigger kwargs and reconstructs the trigger after a restart.
``HttpTrigger.run()`` then issues the HTTP request again. That is safe for
idempotent methods and unsafe for methods that create a new side effect on every
call (the operator default is ``POST``).

HttpOperator treats these methods as idempotent, matching RFC 9110 §9.2.2:

* ``GET``
* ``HEAD``
* ``OPTIONS``
* ``PUT``
* ``DELETE``
* ``TRACE``

``POST``, ``PATCH``, and any other method emit one task-log warning per attempt
when used with deferrable mode.

Silencing the warning
---------------------

If a duplicate request is acceptable for your endpoint, set
``warn_on_non_idempotent=False``:

.. code-block:: python

    HttpOperator(
        task_id="create_resource",
        method="POST",
        endpoint="/resources",
        deferrable=True,
        warn_on_non_idempotent=False,
    )

Safer alternatives for polling or waiting on an HTTP condition are
:class:`~airflow.providers.http.sensors.http.HttpSensor` or an event-driven
trigger such as :class:`~airflow.providers.http.triggers.http.HttpEventTrigger`.
For a one-shot non-idempotent call, prefer ``deferrable=False`` so the worker
issues the request once.

This page is the target of the task-log warning. The warning does not prevent a
duplicate request; it only tells Dag authors that a Triggerer restart can replay
it.
