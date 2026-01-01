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


Deadline Alerts
===============

.. warning::
  Deadline Alerts are new in Airflow 3.1 and should be considered experimental. The feature may be
  subject to changes in 3.2 without warning based on user feedback.

|experimental|

Deadline Alerts allow you to set time thresholds for your Dag runs and automatically respond when those
thresholds are exceeded. You can set up Deadline Alerts by choosing a built-in reference point, setting
an interval, and defining a response using either Airflow's Notifiers or a custom callback function.

Migrating from SLA
------------------

For help migrating from SLA to Deadlines, see the :doc:`migration guide </howto/sla-to-deadlines>`

Creating a Deadline Alert
-------------------------

Creating a Deadline Alert requires three mandatory parameters:

* Reference: When to start counting from
* Interval: How far before or after the reference point to trigger the alert
* Callback: A Callback object which contains a path to a callable and optional kwargs to pass to it if the deadline is exceeded

Here is how Deadlines are calculated:

::

    [Reference] ------ [Interval] ------> [Deadline]
        ^                                     ^
        |                                     |
     Start time                          Trigger point

Below is an example Dag implementation. If the Dag has not finished 15 minutes after it was queued, send a Slack message:

.. code-block:: python

    from datetime import datetime, timedelta
    from airflow import DAG
    from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference
    from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
    from airflow.providers.standard.operators.empty import EmptyOperator

    with DAG(
        dag_id="deadline_alert_example",
        deadline=DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(minutes=15),
            callback=AsyncCallback(
                SlackWebhookNotifier,
                kwargs={
                    "text": "🚨 Dag {{ dag_run.dag_id }} missed deadline at {{ deadline.deadline_time }}. DagRun: {{ dag_run }}"
                },
            ),
        ),
    ):
        EmptyOperator(task_id="example_task")

The timeline for this example would look like this:

::

    |------|-----------|---------|-----------|--------|
        Scheduled    Queued    Started    Deadline
         00:00       00:03      00:05      00:18

.. _built-in-deadline-references:

Using Built-in References
-------------------------

Airflow provides several built-in reference points that you can use with DeadlineAlert:

``DeadlineReference.DAGRUN_QUEUED_AT``
    Measures time from when the Dag run was queued. Useful for monitoring resource constraints.

``DeadlineReference.DAGRUN_LOGICAL_DATE``
    References when the Dag run was scheduled to start. For example, setting an interval of
    ``timedelta(minutes=15)`` would trigger the alert if the Dag hasn't completed 15 minutes
    after it was scheduled to start, regardless of when (or if) it actually began executing.
    Useful for ensuring scheduled Dags complete before their next scheduled run.

``DeadlineReference.FIXED_DATETIME``
    Specifies a fixed point in time. Useful when Dags must complete by a specific time.

``DeadlineReference.AVERAGE_RUNTIME``
    Calculates deadlines based on the average runtime of previous Dag runs. This reference
    analyzes historical execution data to predict when the current run should complete.
    The deadline is set to the current time plus the calculated average runtime plus the interval.
    If insufficient historical data exists, no deadline is created.

    Parameters:
        * ``max_runs`` (int, optional): Maximum number of recent Dag runs to analyze. Defaults to 10.
        * ``min_runs`` (int, optional): Minimum number of completed runs required to calculate average. Defaults to same value as ``max_runs``.

    Example usage:

    .. code-block:: python

        # Use default settings (analyze up to 10 runs, require 10 runs)
        DeadlineReference.AVERAGE_RUNTIME()

        # Analyze up to 20 runs but calculate with minimum 5 runs
        DeadlineReference.AVERAGE_RUNTIME(max_runs=20, min_runs=5)

        # Strict: require exactly 15 runs to calculate
        DeadlineReference.AVERAGE_RUNTIME(max_runs=15, min_runs=15)

Here's an example using average runtime:

.. code-block:: python

    with DAG(
        dag_id="average_runtime_deadline",
        deadline=DeadlineAlert(
            reference=DeadlineReference.AVERAGE_RUNTIME(max_runs=15, min_runs=5),
            interval=timedelta(minutes=30),  # Alert if 30 minutes past average runtime
            callback=AsyncCallback(
                SlackWebhookNotifier,
                kwargs={"text": "🚨 Dag {{ dag_run.dag_id }} is running longer than expected!"},
            ),
        ),
    ):
        EmptyOperator(task_id="data_processing")

If the calculated historical average was 30 minutes, the timeline for this example would look like this:

::

    |------|----------|--------------|--------------|--------|
         Queued     Start            |           Deadline
         09:00      09:05          09:35          10:05
                      |              |              |
                      |--- Average --|-- Interval --|
                           (30 min)      (30 min)


Here's an example using a fixed datetime:

.. code-block:: python

    tomorrow_at_ten = datetime.combine(datetime.now().date() + timedelta(days=1), time(10, 0))

    with DAG(
        dag_id="fixed_deadline_alert",
        deadline=DeadlineAlert(
            reference=DeadlineReference.FIXED_DATETIME(tomorrow_at_ten),
            interval=timedelta(minutes=-30),  # Alert 30 minutes before the reference.
            callback=AsyncCallback(
                SlackWebhookNotifier,
                kwargs={
                    "text": "🚨 Dag {{ dag_run.dag_id }} missed deadline at {{ deadline.deadline_time }}. DagRun: {{ dag_run }}"
                },
            ),
        ),
    ):
        EmptyOperator(task_id="example_task")

The timeline for this example would look like this:

::

    |------|----------|---------|------------|--------|
         Queued     Start    Deadline    Reference
         09:15      09:17     09:30       10:00

.. note::
    Note that since the interval is a negative value, the deadline is before the reference in this case.

Using Callbacks
---------------

When a deadline is exceeded, the callback's callable is executed with the specified kwargs. You can use an
existing :doc:`Notifier </howto/notifications>` or create a custom callable.  A callback must be an
:class:`~airflow.sdk.definitions.deadline.AsyncCallback`, with support coming soon for
:class:`~airflow.sdk.definitions.deadline.SyncCallback`.

Using Built-in Notifiers
^^^^^^^^^^^^^^^^^^^^^^^^

Here's an example using the Slack Notifier if the Dag run has not finished within 30 minutes of it being queued:

.. code-block:: python

    with DAG(
        dag_id="slack_deadline_alert",
        deadline=DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(minutes=30),
            callback=AsyncCallback(
                SlackWebhookNotifier,
                kwargs={
                    "text": "🚨 Dag {{ dag_run.dag_id }} missed deadline at {{ deadline.deadline_time }}. DagRun: {{ dag_run }}"
                },
            ),
        ),
    ):
        EmptyOperator(task_id="example_task")


Creating Custom Callbacks
^^^^^^^^^^^^^^^^^^^^^^^^^

You can create custom callables for more complex handling. If ``kwargs`` are specified in the ``Callback``,
they are passed to the callback function. **Asynchronous callbacks** must be defined somewhere in the
Triggerer's system path.

.. note::
    Regarding Async Custom Deadline callbacks:

    * Async callbacks are executed by the Triggerer, so users must ensure they are importable by the Triggerer.
    * One easy way to do this is to place the callable as a top-level method in a new file in the plugins folder.
      Nested callables are not currently supported.
    * The Triggerer will need to be restarted when a callback is added or changed in order to reload the file.


A **custom asynchronous callback** might look like this:

1. Place this method in ``/files/plugins/deadline_callbacks.py``:

.. code-block:: python

    async def custom_async_callback(**kwargs):
        """Handle deadline violation with custom logic."""
        context = kwargs.get("context", {})
        print(f"Deadline exceeded for Dag {context.get("dag_run", {}).get("dag_id")}!")
        print(f"Context: {context}")
        print(f"Alert type: {kwargs.get("alert_type")}")
        # Additional custom handling here

2. Restart your Triggerer.
3. Place this in a Dag file:

.. code-block:: python

    from datetime import timedelta

    from deadline_callbacks import custom_async_callback

    from airflow import DAG
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference

    with DAG(
        dag_id="custom_deadline_alert",
        deadline=DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(minutes=15),
            callback=AsyncCallback(
                custom_async_callback,
                kwargs={"alert_type": "time_exceeded"},
            ),
        ),
    ):
        EmptyOperator(task_id="example_task")

Templating and Context
^^^^^^^^^^^^^^^^^^^^^^

Currently, a relatively simple version of the Airflow context is passed to callables and Airflow does not run
:ref:`concepts:jinja-templating` on the kwargs. However, Notifiers already run templating with the
provided context as part of their execution. This means that templating can be used when using a Notifier
as long as the variables being templated are included in the simplified context. This currently includes the
ID and the calculated deadline time of the Deadline Alert as well as the data included in the ``GET`` REST API
response for Dag Run. Support for more comprehensive context and templating will be added in future versions.

Deadline Calculation
^^^^^^^^^^^^^^^^^^^^

A deadline's trigger time is calculated by adding the ``interval`` to the datetime returned by
the ``reference``. For ``FIXED_DATETIME`` references, negative intervals can be particularly
useful to trigger the callback *before* the reference time.

For example:

.. code-block:: python

    next_meeting = datetime(2025, 6, 26, 9, 30)

    DeadlineAlert(
        reference=DeadlineReference.FIXED_DATETIME(next_meeting),
        interval=timedelta(hours=-2),
        callback=notify_team,
    )

This will trigger the alert 2 hours before the next meeting starts.

For ``DAGRUN_LOGICAL_DATE``, the interval is typically positive, setting a deadline relative
to when the Dag was scheduled to run. Here's an example:

.. code-block:: python

    DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=1),
        callback=notify_team,
    )

In this case, if a Dag is scheduled to run daily at midnight, the deadline would be triggered
if the Dag hasn't completed by 1:00 AM. This is useful for ensuring that scheduled jobs complete
within a certain timeframe after their intended start time.

The flexibility of combining different references with positive or negative intervals allows
you to create deadlines that suit a wide variety of operational requirements.

Custom References
^^^^^^^^^^^^^^^^^


The built-in references handle most common scenarios. However, you may need to create custom
references for specific integrations like calendars or other data sources. To do this, create
a class that inherits from BaseDeadlineReference, add the ``@deadline_reference`` decorator, and
implement an ``_evaluate_with()`` method.


**Creating a Custom Reference**

.. code-block:: python

    from airflow.models.deadline import ReferenceModels
    from sqlalchemy.orm import Session

    from airflow.sdk.definitions.deadline import DeadlineReference, deadline_reference
    from airflow.sdk.timezone import datetime


    # By default, the evaluate_with method will be executed when the dagrun is created.
    @deadline_reference()
    class MyCustomDecoratedReference(ReferenceModels.BaseDeadlineReference):
        """A custom reference evaluated when Dag runs are created."""

        def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
            # Add your business logic here
            return your_datetime


    # You can specify when evaluate_with will be called by providing a DeadlineReference.TYPES value.
    @deadline_reference(DeadlineReference.TYPES.DAGRUN_QUEUED)
    class MyQueuedReference(ReferenceModels.BaseDeadlineReference):
        """A custom reference evaluated when Dag runs are queued."""

        required_kwargs = {"custom_param"}

        def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
            custom_value = kwargs["custom_param"]
            # Use custom_value in your calculation
            return your_datetime


**Using a Custom Reference in a Dag**

Once registered [see notes below], use your custom references in Dag definitions like any other reference:

.. code-block:: python

    from datetime import timedelta
    from airflow import DAG
    from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference

    with DAG(
        dag_id="custom_reference_example",
        deadline=DeadlineAlert(
            reference=DeadlineReference.MyCustomDecoratedReference(),
            interval=timedelta(hours=2),
            callback=AsyncCallback(my_callback),
        ),
    ):
        # Your tasks here
        ...

**Important Notes:**

* **Timezone Awareness**: Always return timezone-aware datetime objects.
* **Plugin Placement**: One convenient place for custom references is in the plugins directory.
* **API Server Restart**: Restart the Airflow API Server after adding or modifying custom references.
* **Required Parameters**: Use ``required_kwargs`` to specify parameters your reference needs.
* **Database Access**: Use the ``session`` parameter for Airflow database queries if needed.
