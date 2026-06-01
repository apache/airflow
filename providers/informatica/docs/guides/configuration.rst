
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

Configuration
=============

This section describes how to configure the Informatica provider for Apache Airflow.

Connection Setup
----------------

Create an HTTP connection in Airflow for Informatica EDC:

1. **Connection Type**: ``informatica_edc``
2. **Host**: Your EDC server hostname
3. **Port**: EDC server port (typically 9087)
4. **Schema**: ``https`` or ``http``
5. **Login**: EDC username
6. **Password**: EDC password
7. **Extras**: Add the following JSON:

   .. code-block:: json

       {"security_domain": "your_security_domain"}

Configuration Options
---------------------

Add to your ``airflow.cfg``:

.. code-block:: ini

    [informatica]
    # Disable sending events without uninstalling the Informatica Provider
    listener_disabled = False
    # The connection ID to use when no connection ID is provided
    default_conn_id = informatica_edc_default
    # Enable automatic SQL lineage detection (parses the sql attribute of operators)
    auto_lineage_enabled = True
    # Semicolon-separated fully-qualified class names of operators to exclude from lineage
    disabled_for_operators =
    # HTTP request timeout in seconds for EDC API calls
    request_timeout = 30

``auto_lineage_enabled``
~~~~~~~~~~~~~~~~~~~~~~~~

When ``True`` (default), the provider inspects each task's ``sql`` attribute before
execution, parses it with `sqlglot <https://sqlglot.com/>`_, resolves the discovered
tables against the Informatica catalog, and creates lineage links on task success.

Set to ``False`` to rely exclusively on manually declared ``inlets`` and ``outlets``.

``disabled_for_operators``
~~~~~~~~~~~~~~~~~~~~~~~~~~

A semicolon-separated list of fully-qualified Python class names.  Operators whose
class matches an entry in this list are excluded entirely from lineage processing —
both automatic and manual inlets/outlets are ignored.

Example:

.. code-block:: ini

    [informatica]
    disabled_for_operators = airflow.providers.standard.operators.bash.BashOperator;airflow.providers.standard.operators.python.PythonOperator

``request_timeout``
~~~~~~~~~~~~~~~~~~~

Timeout in seconds applied to every HTTP request made to the EDC REST API.
Increase this value for slow or high-latency networks.

Strict Pre-execute Validation
-----------------------------

Listener hooks are best-effort by default. If lineage objects cannot be resolved,
the listener logs a warning and task execution continues.

To fail a task before ``execute()`` when lineage resolution fails, set
``pre_execute=validate_informatica_lineage`` on the operator:

.. code-block:: python

    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    from airflow.providers.informatica.lineage.validation import validate_informatica_lineage

    task = SQLExecuteQueryOperator(
        task_id="transform",
        conn_id="postgres_default",
        sql="INSERT INTO dst SELECT * FROM src",
        pre_execute=validate_informatica_lineage,
    )

Per-task Selective Lineage
--------------------------

You can disable or re-enable automatic lineage on individual tasks (or entire DAGs) at
DAG definition time using the helper functions in ``airflow.providers.informatica.lineage``:

.. code-block:: python

    from airflow.providers.informatica.lineage import (
        disable_informatica_lineage,
        enable_informatica_lineage,
    )

    with DAG("my_dag", ...) as dag:
        task_a = SomeSQLOperator(task_id="task_a", sql="SELECT * FROM orders", ...)
        task_b = SomeSQLOperator(task_id="task_b", sql="SELECT * FROM customers", ...)

        # Disable auto-lineage for task_a only
        disable_informatica_lineage(task_a)

        # Disable auto-lineage for all tasks in a DAG
        disable_informatica_lineage(dag)

SSL and Security
----------------

The connection supports SSL verification control through extras:

.. code-block:: json

    {
        "security_domain": "your_domain",
        "verify_ssl": true
    }

Set ``verify_ssl`` to ``false`` to disable SSL certificate verification.
