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

.. _howto/operator:DatabricksStartWarehouseOperator:
.. _howto/operator:DatabricksStopWarehouseOperator:
.. _howto/operator:DatabricksEditWarehouseOperator:

Databricks SQL warehouse lifecycle operators
============================================

Use :class:`~airflow.providers.databricks.operators.warehouse.DatabricksStartWarehouseOperator`
and :class:`~airflow.providers.databricks.operators.warehouse.DatabricksStopWarehouseOperator`
to start and stop an existing Databricks SQL warehouse through the
`Databricks SQL Warehouses API <https://docs.databricks.com/api/workspace/warehouses>`_.
Both operators require the warehouse ID and use the :ref:`Databricks connection
<howto/connection:databricks>` for authentication.

By default, each operator waits for the requested state: ``RUNNING`` when starting and ``STOPPED``
when stopping. Use ``polling_period_seconds`` to control the polling interval, ``timeout`` to limit
the wait, or ``wait_for_termination=False`` to return after requesting the transition. Set
``deferrable=True`` (or enable ``[operators] default_deferrable``) to wait on the triggerer instead
of holding a worker slot. In deferrable mode the operator records an absolute ``end_time`` from
``timeout`` when it defers, so a triggerer restart does not reset the wait. ``wait_for_termination=False``
still returns immediately and does not defer. Clearing a deferred warehouse wait does not start or
stop the warehouse: Databricks has no cancel API for these transitions, and a cancelled start must
not stop a warehouse the Dag still needs.

Repeated task attempts are safe: an already running warehouse is not started again, and an already
stopped warehouse is not stopped again. If a start is requested while a warehouse is stopping, any
transition rejection from Databricks is propagated to the task.

Edit a SQL warehouse
--------------------

Use :class:`~airflow.providers.databricks.operators.warehouse.DatabricksEditWarehouseOperator`
to change the configuration of an existing warehouse. Pass its ``warehouse_id`` and a non-empty
``json`` dictionary containing the request body for the
`SQL Warehouses edit API <https://docs.databricks.com/api/warehouses/v1/edit-warehouse>`_.
For example, you can set ``cluster_size``, ``min_num_clusters``, ``max_num_clusters``,
``auto_stop_mins``, or ``tags``. Databricks validates the settings and their supported combinations.
Both ``warehouse_id`` and ``json`` are required and have no defaults. Requiring a non-empty
dictionary is an operator validation; it is not an additional requirement documented by Databricks.
The operator does not add defaults for fields omitted from the request body.

``databricks_conn_id`` defaults to ``databricks_default``. The default retry strategy makes up to
``databricks_retry_limit=3`` attempts, including the initial request, with exponential backoff.
``databricks_retry_delay=1`` sets the minimum wait in seconds; the maximum is
``2 ** databricks_retry_limit`` seconds. ``databricks_retry_args`` defaults to ``None``; a non-empty
dictionary replaces the default stop and wait configuration with custom Tenacity settings, while
the hook retains its retryable-error predicate and error-logging callback.

The connection ID, warehouse ID, and nested ``json`` values support templating. Values are sent
without conversion, including ``False`` and ``0``. For numeric or boolean Jinja expressions,
set ``render_template_as_native_obj=True`` on the Dag.

The hook discards the API's empty response object. The task returns ``None`` after the edit request
succeeds and does not produce a ``return_value`` XCom, even with ``do_xcom_push=True``. Pass the
warehouse ID directly to downstream tasks instead of using this task's ``output``.
It does not wait for compute changes
to finish or for the warehouse to reach ``RUNNING``. If the workflow needs a running warehouse,
chain a :class:`~airflow.providers.databricks.operators.warehouse.DatabricksStartWarehouseOperator`
after the edit task. Reaching ``RUNNING`` checks availability; it does not verify that all requested
configuration changes have finished applying.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_sql_warehouse.py
    :language: python
    :start-after: [START howto_operator_databricks_edit_sql_warehouse]
    :end-before: [END howto_operator_databricks_edit_sql_warehouse]

Start a SQL warehouse
---------------------

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_sql_warehouse.py
    :language: python
    :start-after: [START howto_operator_databricks_start_sql_warehouse]
    :end-before: [END howto_operator_databricks_start_sql_warehouse]

Stop a SQL warehouse
--------------------

The stop task uses the ``all_done`` trigger rule so the warehouse is stopped even when an upstream
task fails.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_sql_warehouse.py
    :language: python
    :start-after: [START howto_operator_databricks_stop_sql_warehouse]
    :end-before: [END howto_operator_databricks_stop_sql_warehouse]
