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

The mixin works with both synchronous and deferrable operators. A synchronous operator calls
``execute_resumable``. A deferrable operator calls two smaller methods before ``defer()``, see
:ref:`sdk-resumable-job-mixin-deferrable`.

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
* If the job is in a terminal failure state and ``resume_on_retry`` is set, the mixin calls
  ``resume_failed_job``. If that returns an identifier, the mixin polls it instead of starting over.
  Otherwise it submits a fresh job.

Resuming a failed job
---------------------

.. versionadded:: 3.5.0

Some external systems can rerun a failed job from where it stopped, for example by repairing a
Databricks run or calling ``redrive_execution`` on a Step Functions execution. To use that on retry,
override the optional ``resume_failed_job`` method:

``resume_failed_job(external_id, status, context)``
    Ask the external system to resume the failed job. Return the identifier to poll, which can be the
    same one or a new one. Return ``None`` to submit a fresh job instead. The default returns ``None``.

If ``resume_failed_job`` raises, the engine may still have accepted the request, for example when
the reply times out. Submitting a fresh job then would leave two jobs running. So the mixin reads
the stored job's status again, up to ``resume_status_checks`` times (3 by default), waiting
``resume_status_check_interval`` seconds (10 by default) between reads:

- If the job is running or has succeeded, the mixin watches it instead of submitting a new one.
- If the job still looks failed after every read, the mixin submits a fresh job.
- If the status cannot be read, the try fails with the original error, and the next retry checks
  again.

This check only finds a job resumed under the stored identifier. An engine that resumes under a new
identifier must handle a lost reply in ``resume_failed_job`` itself.

``resume_on_retry`` turns this on or off per task. It defaults to ``True``. Set it to ``False`` to
always submit a fresh job when a retry finds the stored job failed:

.. code-block:: python

    run_spark = MyBatchOperator(
        task_id="run_spark",
        resume_on_retry=False,
    )

Clearing a task discards its stored identifier by default, so a cleared task starts a fresh job. If
the clear keeps task state, the next try treats the stored job like a retry does.

How ``durable`` and ``resume_on_retry`` combine when a retry finds the stored job failed:

* ``durable=True, resume_on_retry=True`` (the default): the mixin resumes the failed job.
* ``durable=True, resume_on_retry=False``: the mixin submits a fresh job.
* ``durable=False``: no identifier is stored, so there is nothing to resume. Every try submits a
  fresh job, whatever ``resume_on_retry`` is set to.

.. note::

   There is a small window between ``submit_job`` returning and ``task_state_store.set`` completing.
   If the worker crashes in that gap, the next retry does not have the identifier and will
   submit a fresh job. For most workloads this window is negligible.

.. note::

   The stored identifier expires after ``[state_store] default_retention_days`` (30 days by
   default), but nothing deletes it automatically, that only happens when someone runs
   ``airflow state-store clean``. Do not run that command while a task's ``retry_delay`` is
   longer than ``default_retention_days``, the identifier could be gone by the next retry,
   causing the operator to submit a fresh job instead of reconnecting.

.. _sdk-resumable-job-mixin-banner:

Durable execution banner
------------------------

When a retry reconnects to a running job, uses the result of a job that already finished, or resumes
a failed job with ``resume_failed_job``, the task logs page shows a banner saying so. The mixin records what each try did under the
``__airflow_durable__`` task state store key, and the banner reads it. A deferrable operator that
reconnects shows the banner without a job status, because the outcome is known only after the
Triggerer fires and nothing records it there. Deployments that set ``[workers] state_store_backend``
do not show the banner, because the record is offloaded with every other value and the banner reads
only what is stored in the metadata database.

.. note::

   Only operators that use ``ResumableJobMixin`` show the banner. ``KubernetesPodOperator``,
   ``MSGraphAsyncOperator`` and ``AgentOperator`` implement durable execution on their own and do
   not record what each try did, so they reconnect without a banner. ``GlueJobOperator`` with
   ``deferrable=True`` uses the mixin but finds a retry's run by searching Glue job runs instead,
   so it shows no banner either.

Example
-------

.. code-block:: python

    from airflow.sdk import BaseOperator, ResumableJobMixin
    from pydantic import JsonValue


    class MyBatchOperator(ResumableJobMixin, BaseOperator):

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

.. _sdk-resumable-job-mixin-deferrable:

Deferrable operators
--------------------

A deferrable operator can't use ``execute_resumable``, because the Triggerer does the polling.
Call these methods before ``defer()`` instead:

* ``get_resume_decision(context)`` checks the stored job and returns a
  :class:`~airflow.sdk.ResumeDecision`. Its ``action`` is ``"submit"``, ``"reconnect"``,
  ``"already_succeeded"`` or ``"resume"``. It never submits, resumes or polls. It records a reconnect
  or an already succeeded job for the :ref:`durable execution banner <sdk-resumable-job-mixin-banner>`.
* ``resume_stored_job(context, decision)`` resumes the failed job on a ``"resume"`` decision and
  returns the id to wait on, or ``None`` when you should submit a new job. It records the resume for
  the banner.
* ``persist_job_id(context, job_id)`` saves a new job's id so a retry can find it.

.. code-block:: python

    def execute(self, context):
        decision = self.get_resume_decision(context)
        if decision.action == "already_succeeded":
            return self.get_job_result(decision.external_id, context)

        job_id = None
        if decision.action == "reconnect":
            job_id = decision.external_id
        elif decision.action == "resume":
            job_id = self.resume_stored_job(context, decision)
        if job_id is None:
            job_id = self.submit_job(context)
            self.persist_job_id(context, job_id)

        self.defer(trigger=MyBatchTrigger(job_id=job_id), method_name="execute_complete")

.. _sdk-resumable-job-mixin-resume-on-retry:

Disabling crash recovery per task
----------------------------------

Set ``durable=False`` on a task to opt out of crash recovery for that specific instance.
The operator will always submit a fresh job on retry, with no ``task_state_store`` interaction:

.. code-block:: python

    run_spark = MyBatchOperator(
        task_id="run_spark",
        durable=False,
    )

This is useful when the external job is not idempotent and you want Airflow to always submit a
clean run rather than reconnect to a prior submission.

The default is ``True``. ``durable`` is owned by the mixin — operators do not need to
redeclare it. ``default_args`` injection and ``.partial()`` work automatically.

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
