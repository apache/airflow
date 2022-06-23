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


Qubole
======
`Qubole <https://www.qubole.com/>`__ is an open, simple, and secure data lake platform for machine learning, streaming and adhoc analytics.
Qubole delivers a Self-Service Platform for Big Data Analytics built on Amazon Web Services, Microsoft and Google Clouds.

Airflow provides operators to execute tasks (commands) on QDS and perform checks against Qubole Commands.
Also, there are provided sensors that waits for a file, folder or partition to be present in cloud storage and check for its presence via QDS APIs


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:QuboleOperator:

Execute tasks
^^^^^^^^^^^^^

To run following commands use
:class:`~airflow.providers.qubole.operators.qubole.QuboleOperator`.

Run Hive command
""""""""""""""""

To run query that shows all tables you can use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_hive_query]
    :end-before: [END howto_operator_qubole_run_hive_query]

Also you can run script that locates in the bucket by passing path to query file

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_hive_script]
    :end-before: [END howto_operator_qubole_run_hive_script]

Run Hadoop command
""""""""""""""""""

To run jar file in your Hadoop cluster use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_hadoop_jar]
    :end-before: [END howto_operator_qubole_run_hadoop_jar]

Run Pig command
"""""""""""""""

To run script script in *Pig Latin* in your Hadoop cluster use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_pig_script]
    :end-before: [END howto_operator_qubole_run_pig_script]

Run Shell command
"""""""""""""""""

To run Shell-script script use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_shell_script]
    :end-before: [END howto_operator_qubole_run_shell_script]

Run Presto command
""""""""""""""""""

To run query using Presto use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_presto_query]
    :end-before: [END howto_operator_qubole_run_presto_query]

Run DB commands
"""""""""""""""

To run query as `DbTap <https://docs.qubole.com/en/latest/rest-api/dbtap_api/index.html>`_ use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_db_tap_query]
    :end-before: [END howto_operator_qubole_run_db_tap_query]

To run DB export command use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_db_export]
    :end-before: [END howto_operator_qubole_run_db_export]

To run DB import command use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_db_import]
    :end-before: [END howto_operator_qubole_run_db_import]

Run Spark commands
""""""""""""""""""

To run Scala script as a Spark job use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qubole_run_spark_scala]
    :end-before: [END howto_operator_qubole_run_spark_scala]


.. _howto/operator:QuboleFileSensor:

File sensor
^^^^^^^^^^^

Usage examples of
:class:`~airflow.providers.qubole.sensors.qubole.QuboleFileSensor`.

File or directory existence
"""""""""""""""""""""""""""

To wait for file or directory existence in cluster use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_qubole_run_file_sensor]
    :end-before: [END howto_sensor_qubole_run_file_sensor]


.. _howto/operator:QubolePartitionSensor:

Partition sensor
^^^^^^^^^^^^^^^^

Usage examples of
:class:`~airflow.providers.qubole.sensors.qubole.QubolePartitionSensor`.

Partition existence
"""""""""""""""""""

To wait for table partition existence in cluster use

.. exampleinclude:: /../../tests/system/providers/qubole/example_qubole_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_qubole_run_partition_sensor]
    :end-before: [END howto_sensor_qubole_run_partition_sensor]


Reference
^^^^^^^^^

For further information, look at:

* `Qubole Data Service Python SDK <https://github.com/qubole/qds-sdk-py>`__
* `Product Documentation <https://docs.qubole.com/en/latest/>`__
