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

.. _howto/operator:AirflowFlyteOperator:

AirflowFlyteOperator
====================

Use the :class:`~airflow.providers.flyte.operators.AirflowFlyteOperator` to
trigger a task/workflow in Flyte.

Using the Operator
^^^^^^^^^^^^^^^^^^

The AirflowFlyteOperator requires a ``flyte_conn_id`` to fetch all the connection-related
parameters that may be useful to instantiate ``FlyteRemote``. Also, you must give a
``launchplan_name`` — to trigger a workflow, or ``task_name`` — to trigger a task, and you can give a
handful of other values that are optional, such as ``project``, ``domain``, ``max_parallelism``,
``raw_data_prefix``, ``assumable_iam_role``, ``kubernetes_service_account``, ``version``, ``inputs``, and ``timeout``.

The executions will be triggered synchronously by default on Flyte. You can set the ``asynchronous`` parameter to
``True`` to trigger the executions asynchronously.

An example where the execution is triggered synchronously:

.. exampleinclude:: /../../airflow/providers/flyte/example_dags/example_flyte.py
    :language: python
    :start-after: [START howto_operator_flyte_synchronous]
    :end-before: [END howto_operator_flyte_synchronous]

An example where the execution is triggered asynchronously:

.. exampleinclude:: /../../airflow/providers/flyte/example_dags/example_flyte.py
    :language: python
    :start-after: [START howto_operator_flyte_asynchronous]
    :end-before: [END howto_operator_flyte_asynchronous]
