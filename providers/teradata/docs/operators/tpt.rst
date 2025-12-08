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

.. _howto/operator:DdlOperator:

DdlOperator
===========

The ``DdlOperator`` is an Airflow operator designed to execute Data Definition Language (DDL) statements on Teradata databases. It provides a robust way to create, alter, or drop database objects as part of your data pipelines.

.. note::

    The ``DdlOperator`` requires the ``Teradata Parallel Transporter (TPT)`` package from Teradata Tools and Utilities (TTU)
    to be installed on the machine where the ``tbuild`` command will run (either local or remote).
    Ensure that the ``tbuild`` executable is available in the system's ``PATH``.
    Refer to the official Teradata documentation for installation, configuration, and security best practices.

**Key Features:**

- Executes DDL SQL statements (CREATE, ALTER, DROP, etc.)
- Works with single statements or batches of multiple DDL operations
- Integrates with Airflow's connection management for secure database access
- Provides comprehensive logging of execution results
- Supports both local and remote execution via SSH

When you need to manage database schema changes, create temporary tables, or clean up data structures as part of your workflow, the ``DdlOperator`` offers a streamlined approach that integrates seamlessly with your Airflow DAGs.

Prerequisite
------------

Make sure your Teradata Airflow connection is defined with the required fields:

- ``host``
- ``login``
- ``password``

You can define a remote host with a separate SSH connection using the ``ssh_conn_id``.

Ensure that the ``Teradata Parallel Transporter (TPT)`` package is installed on the machine where TdLoadOperator will execute commands. This can be:

- The **local machine** where Airflow runs the task, for local execution.
- A **remote host** accessed via SSH, for remote execution.

If executing remotely, ensure that an SSH server (e.g., ``sshd``) is running and accessible on the remote machine, and that the ``tbuild`` executable is available in the system's ``PATH``.

.. note::

    For improved security, it is **highly recommended** to use
    **private key-based SSH authentication** (SSH key pairs) instead of username/password
    for the SSH connection.

    This avoids password exposure, enables seamless automated execution, and enhances security.

    See the Airflow SSH Connection documentation for details on configuring SSH keys:
    https://airflow.apache.org/docs/apache-airflow/stable/howto/connection/ssh.html


To execute DDL operations in a Teradata database, use the
:class:`~airflow.providers.teradata.operators.ddl.DdlOperator`.

Handling Escape Sequences for Embedded Quotes
----------------------------------------------

When working with DDL statements that contain embedded quotes, it's important to understand how escape sequences are handled differently between the DAG definition and the SQL execution:

**In DAG Definition (Python):**
- Use backslash escape sequences: ``\"`` for double quotes, ``\'`` for single quotes
- Python string literals require backslash escaping

**In SQL Execution (Teradata):**
- SQL standard requires doubling quotes when enclosed within the same quote type
- Single quotes in single-quoted strings: ``'Don''t'``
- Double quotes in double-quoted identifiers: ``"My""Table"``

**Example:**

.. code-block:: python

    # In your DAG - use Python escape sequences
    ddl_with_quotes = DdlOperator(
        task_id="create_table_with_quotes",
        ddl=[
            "CREATE TABLE test_table (col1 VARCHAR(50) DEFAULT '\"quoted_value\"')",
            "INSERT INTO test_table VALUES ('It''s a test')",  # Note the doubled single quotes
        ],
        teradata_conn_id="teradata_default",
    )

**Key Points:**
- When defining DDL statements in Python strings, use standard Python escape sequences
- The operator automatically handles the conversion for TPT script generation
- For SQL string literals containing quotes, follow SQL standards (double the quotes)
- Test your DDL statements carefully when they contain complex quoting

Key Operation Examples with DdlOperator
---------------------------------------

Dropping tables in Teradata
---------------------------
You can use the DdlOperator to drop tables in Teradata. The following example demonstrates how to drop multiple tables:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_drop_table]
    :end-before: [END ddl_operator_howto_guide_drop_table]

Creating tables in Teradata
---------------------------
You can use the DdlOperator to create tables in Teradata. The following example demonstrates how to create multiple tables:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_create_table]
    :end-before: [END ddl_operator_howto_guide_create_table]

Creating an index on a Teradata table
-------------------------------------
You can use the DdlOperator to create an index on a Teradata table. The following example demonstrates how to create an index:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_create_index]
    :end-before: [END ddl_operator_howto_guide_create_index]

Renaming a table in Teradata
----------------------------
You can use the DdlOperator to rename a table in Teradata. The following example demonstrates how to rename a table:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_rename_table]
    :end-before: [END ddl_operator_howto_guide_rename_table]

Dropping an index in Teradata
-----------------------------
You can use the DdlOperator to drop an index in Teradata. The following example demonstrates how to drop an index:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_drop_index]
    :end-before: [END ddl_operator_howto_guide_drop_index]

Altering a table in Teradata
----------------------------
You can use the DdlOperator to alter a table in Teradata. The following example demonstrates how to add a column:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :dedent: 4
    :start-after: [START ddl_operator_howto_guide_alter_table]
    :end-before: [END ddl_operator_howto_guide_alter_table]

The complete Teradata Operator DAG
----------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_tpt.py
    :language: python
    :start-after: [START ddl_operator_howto_guide]
    :end-before: [END ddl_operator_howto_guide]
