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

Migrating from SLA to Deadline Alerts
=====================================

Two Different Paradigms
-----------------------

While the goal of the **SLA** and **Deadline Alerts** features are very similar, they use two very different approaches.
This guide will lay out the major differences and help you decide on the best approach for your use case.

To begin with, we'll start by explaining the two approaches then go into how to find the right Deadline for your use case.

SLA
^^^

When the dag run **finishes**, check the current time.  If the time is greater than (logical_date + sla) then
execute ``sla_miss_callback``.  If the Dag run never finishes, the SLA is never checked.

Deadline Alerts
^^^^^^^^^^^^^^^

When a Dag run **starts**, calculate and store (:ref:`DeadlineReference <built-in-deadline-references>` + interval).
The scheduler loop then checks periodically (default 5 seconds, set by ``scheduler_heartbeat_sec``) if any of those
times have passed then execute ``callback(**kwargs)``.

The most direct migration path would be to use the ``DeadlineReference.DAGRUN_LOGICAL_DATE`` reference, but note that
the major change is that the Deadline's callback will execute "immediately" (within ``scheduler_heartbeat_sec`` of the
calculated expiration time) and not wait until the Dag finishes first.

Equivalent Example Dags
-----------------------

Below is a Dag using a 1-hour SLA, followed by an equivalent Dag using Deadline Alerts.

SLA Example
^^^^^^^^^^^

.. code-block:: python

  with DAG(
      "minimal_sla_example",
      default_args={"sla": timedelta(hours=1)},
      sla_miss_callback=SlackWebhookNotifier(
          text="SLA missed for {{ dag_run.dag_id }}",
      ),
  ):
      BashOperator(task_id="long_task", bash_command="sleep 3600")

Deadline Alerts Example
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

  with DAG(
      "minimal_deadline_example",
      deadline=DeadlineAlert(
          reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
          interval=timedelta(hours=1),
          callback=AsyncCallback(
              SlackWebhookNotifier,
              kwargs={
                  text: "Deadline missed for {{ dag_run.dag_id }}",
              },
          ),
      ),
  ):
      BashOperator(task_id="long_task", bash_command="sleep 3600")


Further Reading
---------------

For more details on the Deadline Alerts feature, see the :doc:`how-to guide </howto/deadline-alerts>`.
