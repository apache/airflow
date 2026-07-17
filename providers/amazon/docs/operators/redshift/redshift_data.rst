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

====================
Amazon Redshift Data
====================

`Amazon Redshift <https://aws.amazon.com/redshift/>`__ manages all the work of setting up, operating, and scaling a data warehouse:
provisioning capacity, monitoring and backing up the cluster, and applying patches and upgrades to
the Amazon Redshift engine. You can focus on using your data to acquire new insights for your
business and customers.

Prerequisite Tasks
------------------

.. include:: ../../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:RedshiftDataOperator:

Execute a statement on an Amazon Redshift cluster
=================================================

Use the :class:`RedshiftDataOperator <airflow.providers.amazon.aws.operators.redshift_data>` to execute
statements against an Amazon Redshift cluster.

This differs from ``RedshiftSQLOperator`` in that it allows users to query and retrieve data via the AWS API and avoid
the necessity of a Postgres connection.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_redshift.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_data]
    :end-before: [END howto_operator_redshift_data]

Reuse a session when executing multiple statements
==================================================

Specify the ``session_keep_alive_seconds`` parameter on an upstream task. In a downstream task, get the session ID from
the XCom and pass it to the ``session_id`` parameter. This is useful when you work with temporary tables.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_redshift.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_data_session_reuse]
    :end-before: [END howto_operator_redshift_data_session_reuse]

Durable execution
==================

``RedshiftDataOperator`` submits a statement and then polls it to completion on the worker. By
default the operator runs in a *durable* mode that makes this crash-safe: the Redshift statement
id is persisted to :doc:`task state store <apache-airflow:core-concepts/task-state-store>` before
polling begins, so if the worker crashes or is preempted and the task is retried, the operator
reconnects to the statement that is already executing in Redshift instead of resubmitting the SQL.

On retry the operator checks the prior statement's state:

* if it is still running, the operator reconnects and continues polling
* if it already succeeded, the operator returns immediately without resubmitting
* if it failed terminally, or its id has expired and is no longer found, the operator submits the
  SQL fresh

This protection also applies when ``wait_for_completion=False`` -- even though that task attempt
never polls at all, a retry after a successful submission still reconnects rather than
resubmitting, since the statement id is persisted immediately after submission regardless of
whether the task waits for it to finish.

Durable execution requires Airflow 3.3 or newer, since it relies on the task state store. On
earlier Airflow versions the flag is a no-op and the operator always submits fresh SQL on retry,
exactly as before. If the task state store is unavailable at runtime, the operator logs that
crash recovery is disabled and behaves the same way.

To opt out and always submit fresh SQL on retry, set ``durable=False``:

.. code-block:: python

  statement = RedshiftDataOperator(
      task_id="redshift_data",
      database="dev",
      sql="SELECT * FROM table",
      cluster_identifier="cluster_identifier",
      durable=False,
  )

Durable execution applies to the synchronous path. When ``deferrable=True`` is set, the Triggerer
already tracks the statement across the wait, so deferrable mode takes precedence and ``durable``
has no effect.

Reference
---------

 * `AWS boto3 library documentation for Amazon Redshift Data <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html>`__
