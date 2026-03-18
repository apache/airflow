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

.. _howto/operator:BteqOperator:

BteqOperator
============

The :class:`~airflow.providers.teradata.operators.bteq.BteqOperator` enables execution of SQL statements or BTEQ (Basic Teradata Query) scripts using the Teradata BTEQ utility, which can be installed either locally or accessed remotely via SSH.

This is useful for executing administrative operations, batch queries, or ELT tasks in Teradata environments using the Teradata BTEQ utility.

.. note::

   This operator requires the Teradata Tools and Utilities (TTU) including the ``bteq`` binary to be installed
   and accessible via the system's ``PATH`` (either locally or on the remote SSH host).

Use the ``BteqOperator`` when you want to:

- Run parameterized or templated SQL/BTEQ scripts
- Connect securely to Teradata with Airflow connections
- Execute queries via SSH on remote systems with BTEQ installed

Prerequisite
------------

Make sure your Teradata Airflow connection is defined with the required fields:

- ``host``
- ``login``
- ``password``
- Optional: ``database``, etc.

You can define a remote host with a separate SSH connection using the ``ssh_conn_id``.


Ensure that the Teradata BTEQ utility is installed on the machine where the SQL statements or scripts will be executed. This could be:

- The **local machine** where Airflow runs the task, for local execution.
- The **remote host** accessed via SSH, for remote execution.

If executing remotely, also ensure that an SSH server (e.g., ``sshd``) is running and accessible on the remote machine.


.. note::

   For improved security, it is **highly recommended** to use
   **private key-based SSH authentication** (SSH key pairs) instead of username/password
   for the SSH connection.

   This avoids password exposure, enables seamless automated execution, and enhances security.

   See the Airflow SSH Connection documentation for details on configuring SSH keys:
   https://airflow.apache.org/docs/apache-airflow/stable/howto/connection/ssh.html



To execute arbitrary SQL or BTEQ commands in a Teradata database, use the
:class:`~airflow.providers.teradata.operators.bteq.BteqOperator`.

Common Database Operations with BteqOperator when BTEQ is installed on local machine
-------------------------------------------------------------------------------------

Creating a Teradata database table
----------------------------------

You can use the BteqOperator to create tables in a Teradata database. The following example demonstrates how to create a simple employee table:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_create_table]
    :end-before: [END bteq_operator_howto_guide_create_table]

The BTEQ script within this operator handles the table creation, including defining columns, data types, and constraints.


Inserting data into a Teradata database table
---------------------------------------------

The following example demonstrates how to populate the ``my_employees`` table with sample employee records:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_populate_table]
    :end-before: [END bteq_operator_howto_guide_populate_table]

This BTEQ script inserts multiple rows into the table in a single operation, making it efficient for batch data loading.


Exporting data from a Teradata database table to a file
-------------------------------------------------------

The BteqOperator makes it straightforward to export query results to a file. This capability is valuable for data extraction, backups, and transferring data between systems. The following example demonstrates how to query the employee table and export the results:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_export_data_to_a_file]
    :end-before: [END bteq_operator_howto_guide_export_data_to_a_file]

The BTEQ script above handles the data export with options for formatting, file location specification, and error handling during the export process.


Fetching and processing records from your Teradata database
-----------------------------------------------------------

You can use BteqOperator to query and retrieve data from your Teradata tables. The following example demonstrates
how to fetch specific records from the employee table with filtering and formatting:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_get_it_employees]
    :end-before: [END bteq_operator_howto_guide_get_it_employees]

Executing a BTEQ script with the BteqOperator
---------------------------------------------

You can use BteqOperator to execute a BTEQ script directly. This is useful for running complex queries or scripts that require multiple SQL statements or specific BTEQ commands.

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_bteq_file_input]
    :end-before: [END bteq_operator_howto_guide_bteq_file_input]


Common Database Operations with BteqOperator when BTEQ is installed on remote machine
-------------------------------------------------------------------------------------

Make sure SSH connection is defined with the required fields to connect to remote machine:

- ``remote_host``
- ``username``
- ``password``
- Optional: ``key_file``, ``private_key``, ``conn_timeout``, etc.

Creating a Teradata database table
----------------------------------

You can use the BteqOperator to create tables in a Teradata database. The following example demonstrates how to create a simple employee table:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_remote_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_create_table]
    :end-before: [END bteq_operator_howto_guide_create_table]

The BTEQ script within this operator handles the table creation, including defining columns, data types, and constraints.


Inserting data into a Teradata database table
---------------------------------------------

The following example demonstrates how to populate the ``my_employees`` table with sample employee records:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_remote_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_populate_table]
    :end-before: [END bteq_operator_howto_guide_populate_table]

This BTEQ script inserts multiple rows into the table in a single operation, making it efficient for batch data loading.


Exporting data from a Teradata database table to a file
-------------------------------------------------------

The BteqOperator makes it straightforward to export query results to a file. This capability is valuable for data extraction, backups, and transferring data between systems. The following example demonstrates how to query the employee table and export the results:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_remote_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_export_data_to_a_file]
    :end-before: [END bteq_operator_howto_guide_export_data_to_a_file]

The BTEQ script above handles the data export with options for formatting, file location specification, and error handling during the export process.


Fetching and processing records from your Teradata database
-----------------------------------------------------------

You can use BteqOperator to query and retrieve data from your Teradata tables. The following example demonstrates
how to fetch specific records from the employee table with filtering and formatting:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_remote_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_get_it_employees]
    :end-before: [END bteq_operator_howto_guide_get_it_employees]

This example shows how to:
- Execute a SELECT query with WHERE clause filtering
- Format the output for better readability
- Process the result set within the BTEQ script
- Handle empty result sets appropriately

Executing a BTEQ script with the BteqOperator when BTEQ script file is on remote machine
----------------------------------------------------------------------------------------

You can use BteqOperator to execute a BTEQ script directly when file is on remote machine.

.. exampleinclude:: /../../teradata/tests/system/teradata/example_remote_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_bteq_file_input]
    :end-before: [END bteq_operator_howto_guide_bteq_file_input]


Using Conditional Logic with BteqOperator
-----------------------------------------

The BteqOperator supports executing conditional logic within your BTEQ scripts. This powerful feature lets you create dynamic, decision-based workflows that respond to data conditions or processing results:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_conditional_logic]
    :end-before: [END bteq_operator_howto_guide_conditional_logic]

Conditional execution enables more intelligent data pipelines that can adapt to different scenarios without requiring separate Dag branches.


Error Handling in BTEQ Scripts
------------------------------

The BteqOperator allows you to implement comprehensive error handling within your BTEQ scripts:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_error_handling]
    :end-before: [END bteq_operator_howto_guide_error_handling]

This approach lets you catch and respond to errors at the BTEQ script level, providing more granular control over error conditions and enabling appropriate recovery actions.


Dropping a Teradata Database Table
----------------------------------

When your workflow completes or requires cleanup, you can use the BteqOperator to drop database objects. The following example demonstrates how to drop the ``my_employees`` table:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :dedent: 4
    :start-after: [START bteq_operator_howto_guide_drop_table]
    :end-before: [END bteq_operator_howto_guide_drop_table]


The complete Teradata Operator Dag
----------------------------------

When we put everything together, our Dag should look like this:

.. exampleinclude:: /../../teradata/tests/system/teradata/example_bteq.py
    :language: python
    :start-after: [START bteq_operator_howto_guide]
    :end-before: [END bteq_operator_howto_guide]
