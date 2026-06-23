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

.. _sdk-resumable-job-mixin:

ResumableJobMixin
=================

.. versionadded:: 3.3.0

:class:`~airflow.sdk.ResumableJobMixin` is a mixin for operators that submit long-running jobs
to an external system and poll for its completion. It makes the operator crash-safe by persisting
the external job identifier to task state store before polling begins. If the worker is restarted
or the host is preempted, the next retry reconnects to the already running job instead of submitting
a duplicate.

This mixin is not a replacement for deferrable operators. Deferrable operators free the
worker slot during polling and are the recommended approach when a Triggerer is available.
Use this mixin when you want crash safety on an existing synchronous operator without
migrating to the deferrable pattern, or when your deployment does not include a Triggerer.

For guidance on choosing between deferrable, resumable, and async approaches, see
:ref:`apache-airflow:concepts-resumable-tasks`.

Interface
---------

Subclasses must implement these six methods:

.. code-block:: python

    def submit_job(self, context: Context) -> JsonValue: ...
    def get_job_status(self, external_id: JsonValue, context: Context) -> str: ...
    def is_job_active(self, status: str) -> bool: ...
    def is_job_succeeded(self, status: str) -> bool: ...
    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None: ...
    def get_job_result(self, external_id: JsonValue, context: Context) -> Any: ...

.. _sdk-resumable-job-mixin-implementing:

Implementing the mixin
----------------------

Add inheritance to :class:`~airflow.sdk.ResumableJobMixin` in your operator class, then call
``execute_resumable(context)`` from your ``execute`` method. The mixin requires you to
implement six methods that describe how to interact with your external system:

``submit_job(context)``
    Submit the job and return its external identifier. The returned value is stored in
    ``task_state_store`` and passed back to the other methods on retry. Return ``None`` only if
    the external system does not provide a trackable identifier; in that case the mixin
    cannot provide crash safety and will resubmit on every retry.

``get_job_status(external_id, context)``
    Query the external system for the current job status. Return a raw string from
    the external system. This method is called on retry to determine whether the job
    is still running, succeeded, or failed.

``is_job_active(status)``
    Return ``True`` if the job is still running and can be reconnected to. ``status`` is the
    raw string returned by ``get_job_status``, a backend-specific value from the external
    system, not an Airflow state::

        def is_job_active(self, status: str) -> bool:
            return status in ("RUNNING", "PENDING", "ACCEPTED")

``is_job_succeeded(status)``
    Return ``True`` if the job completed successfully. ``status`` is the same raw string
    from the external system::

        def is_job_succeeded(self, status: str) -> bool:
            return status == "SUCCEEDED"

``poll_until_complete(external_id, context)``
    Block until the job reaches a terminal state. Raise on failure.

``get_job_result(external_id, context)``
    Return the job result after successful completion. Return ``None`` if not applicable.

How it works
------------

On the first run, after ``submit_job`` returns the external identifier, the mixin persists
that identifier to ``task_state_store`` before calling ``poll_until_complete``. If the worker
crashes during polling, the next retry reads the stored identifier and calls ``get_job_status``
to check the current state of the job:

* If the job is still running, the mixin calls ``poll_until_complete`` to reconnect and continue
  waiting.
* If the job already completed successfully, the mixin calls ``get_job_result`` and returns
  immediately without resubmitting.
* If the job is in a terminal failure state, the mixin falls through and submits a fresh job.

.. note::

   There is a small window between ``submit_job`` returning and ``task_state_store.set`` completing.
   If the worker crashes in that gap, the next retry does not have the identifier and will
   submit a fresh job. For most workloads this window is negligible.

Example
-------

.. code-block:: python

    from airflow.sdk import BaseOperator, ResumableJobMixin
    from pydantic import JsonValue


    class MyBatchOperator(BaseOperator, ResumableJobMixin):

        external_id_key = "batch_job_id"

        def execute(self, context):
            return self.execute_resumable(context)

        def submit_job(self, context) -> JsonValue:
            return self.hook.submit_batch(...)

        def get_job_status(self, external_id: JsonValue, context) -> str:
            return self.hook.get_status(external_id)

        def is_job_active(self, status: str) -> bool:
            return status in ("RUNNING", "PENDING", "QUEUED")

        def is_job_succeeded(self, status: str) -> bool:
            return status == "SUCCEEDED"

        def poll_until_complete(self, external_id: JsonValue, context) -> None:
            self.hook.wait(external_id)

        def get_job_result(self, external_id: JsonValue, context):
            return None

.. _sdk-resumable-job-mixin-external-id-key:

External ID key
---------------

The ``external_id_key`` class attribute controls which key is used to store the job identifier
in ``task_state_store``. The default value is ``"remote_job_id"``. You can override it on your
subclass to use a more descriptive name:

.. code-block:: python

    class MyBatchOperator(ResumableJobMixin, BaseOperator):
        external_id_key = "batch_job_id"

.. warning::

   Do not rename ``external_id_key`` on an operator that is already deployed and has
   in-flight task instances. The old key is already stored in the task state store under the
   previous name. A rename causes the mixin to treat every active retry as a fresh submission,
   defeating the crash-safety guarantee.
