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


Qubole Check
============
`Qubole <https://www.qubole.com/>`__ is an open, simple, and secure data lake platform for machine learning, streaming and adhoc analytics.
Qubole delivers a Self-Service Platform for Big Data Analytics built on Amazon Web Services, Microsoft and Google Clouds.

Airflow provides operators to execute tasks (commands) on QDS and perform checks against Qubole Commands.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:QuboleCheckOperator:

Perform checks
^^^^^^^^^^^^^^

To performs checks against Qubole Commands you can use
:class:`~airflow.providers.qubole.operators.qubole.QuboleCheckOperator`.

Operator expects a command that will be executed on QDS.
By default, each value on first row of the result of this Qubole Command is evaluated using python ``bool`` casting.
If any of the values return ``False``, the check is failed and errors out.


Reference
^^^^^^^^^

For further information, look at:

* `Qubole Data Service Python SDK <https://github.com/qubole/qds-sdk-py>`__
* `Product Documentation <https://docs.qubole.com/en/latest/>`__
