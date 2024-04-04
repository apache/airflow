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

==============
Amazon Bedrock
==============

`Amazon Bedrock <https://aws.amazon.com/bedrock/>`__ is a fully managed service that
offers a choice of high-performing foundation models (FMs) from leading AI companies
like AI21 Labs, Anthropic, Cohere, Meta, Mistral AI, Stability AI, and Amazon via a
single API, along with a broad set of capabilities you need to build generative AI
applications with security, privacy, and responsible AI.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:BedrockInvokeModelOperator:

Invoke an existing Amazon Bedrock Model
=======================================

To invoke an existing Amazon Bedrock model, you can use
:class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockInvokeModelOperator`.

Note that every model family has different input and output formats.
For example, to invoke a Meta Llama model you would use:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_invoke_llama_model]
    :end-before: [END howto_operator_invoke_llama_model]

To invoke an Amazon Titan model you would use:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_invoke_titan_model]
    :end-before: [END howto_operator_invoke_titan_model]

For details on the different formats, see `Inference parameters for foundation models <https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters.html>`__

.. _howto/operator:BedrockCustomizeModelOperator:

Customize an existing Amazon Bedrock Model
==========================================

To create a fine-tuning job to customize a base model, you can use
:class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockCustomizeModelOperator`.

Model-customization jobs are asynchronous and the completion time depends on the base model
and the training/validation data size. To monitor the state of the job, you can use the
"model_customization_job_complete" Waiter, the
:class:`~airflow.providers.amazon.aws.sensors.bedrock.BedrockCustomizeModelCompletedSensor` Sensor,
or the :class:`~airflow.providers.amazon.aws.triggers.BedrockCustomizeModelCompletedTrigger` Trigger.


.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_customize_model]
    :end-before: [END howto_operator_customize_model]


Sensors
-------

.. _howto/sensor:BedrockCustomizeModelCompletedSensor:

Wait for an Amazon Bedrock customize model job
==============================================

To wait on the state of an Amazon Bedrock customize model job until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.sensors.bedrock.BedrockCustomizeModelCompletedSensor`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_customize_model]
    :end-before: [END howto_sensor_customize_model]

Reference
---------

* `AWS boto3 library documentation for Amazon Bedrock <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bedrock.html>`__
