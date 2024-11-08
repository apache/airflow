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

==================
AWS Step Functions
==================

`AWS Step Functions <https://aws.amazon.com/step-functions/>`__ makes it easy to coordinate the components
of distributed applications as a series of steps in a visual workflow. You can quickly build and run state
machines to execute the steps of your application in a reliable and scalable fashion.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:StepFunctionStartExecutionOperator:

Start an AWS Step Functions state machine execution
===================================================

To start a new AWS Step Functions state machine execution you can use
:class:`~airflow.providers.amazon.aws.operators.step_function.StepFunctionStartExecutionOperator`.
You can also run this operator in deferrable mode by setting ``deferrable`` param to ``True``.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_step_functions.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_step_function_start_execution]
    :end-before: [END howto_operator_step_function_start_execution]

.. _howto/operator:StepFunctionGetExecutionOutputOperator:

Get an AWS Step Functions execution output
==========================================

To fetch the output from an AWS Step Function state machine execution you can
use :class:`~airflow.providers.amazon.aws.operators.step_function.StepFunctionGetExecutionOutputOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_step_functions.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_step_function_get_execution_output]
    :end-before: [END howto_operator_step_function_get_execution_output]

Sensors
-------

.. _howto/sensor:StepFunctionExecutionSensor:

Wait on an AWS Step Functions state machine execution state
===========================================================

To wait on the state of an AWS Step Function state machine execution until it reaches a terminal state you can
use :class:`~airflow.providers.amazon.aws.sensors.step_function.StepFunctionExecutionSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_step_functions.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_step_function_execution]
    :end-before: [END howto_sensor_step_function_execution]

References
----------

* `AWS boto3 library documentation for Step Functions <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html>`__
