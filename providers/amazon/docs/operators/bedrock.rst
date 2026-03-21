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
like ``AI21 Labs``, ``Anthropic``, ``Cohere``, ``Meta``, ``Mistral AI``, ``Stability AI``,
and ``Amazon`` via a single API, along with a broad set of capabilities you need to build generative AI
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

Note that every model family has different input and output formats.  Some examples are included below, but
for details on the different formats, see
`Inference parameters for foundation models <https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters.html>`__

For example, to invoke a Meta Llama model you would use:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_invoke_llama_model]
    :end-before: [END howto_operator_invoke_llama_model]

To invoke an Amazon Titan model you would use:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_invoke_titan_model]
    :end-before: [END howto_operator_invoke_titan_model]

To invoke a Claude Sonnet model using the Messages API you would use:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_invoke_claude_model]
    :end-before: [END howto_operator_invoke_claude_model]


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


.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_customize_model]
    :end-before: [END howto_operator_customize_model]

.. _howto/operator:BedrockCreateProvisionedModelThroughputOperator:

Provision Throughput for an existing Amazon Bedrock Model
=========================================================

To create a provisioned throughput with dedicated capacity for a foundation
model or a fine-tuned model, you can use
:class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockCreateProvisionedModelThroughputOperator`.

Provision throughput jobs are asynchronous. To monitor the state of the job, you can use the
"provisioned_model_throughput_complete" Waiter, the
:class:`~airflow.providers.amazon.aws.sensors.bedrock.BedrockProvisionModelThroughputCompletedSensor` Sensor,
or the :class:`~airflow.providers.amazon.aws.triggers.BedrockProvisionModelThroughputCompletedSensorTrigger`
Trigger.


.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_provision_throughput]
    :end-before: [END howto_operator_provision_throughput]

.. _howto/operator:BedrockCreateKnowledgeBaseOperator:

Create an Amazon Bedrock Knowledge Base
========================================

To create an Amazon Bedrock Knowledge Base, you can use
:class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockCreateKnowledgeBaseOperator`.

For more information on which models support embedding data into a vector store, see
https://docs.aws.amazon.com/bedrock/latest/userguide/knowledge-base-supported.html

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bedrock_create_knowledge_base]
    :end-before: [END howto_operator_bedrock_create_knowledge_base]

.. _howto/operator:BedrockDeleteKnowledgeBase:

Delete an Amazon Bedrock Knowledge Base
=======================================

Deleting a Knowledge Base is a simple boto API call and can be done in a TaskFlow task like the example below.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :start-after: [START howto_operator_bedrock_delete_knowledge_base]
    :end-before: [END howto_operator_bedrock_delete_knowledge_base]

.. _howto/operator:BedrockCreateDataSourceOperator:

Create an Amazon Bedrock Data Source
====================================

To create an Amazon Bedrock Data Source, you can use
:class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockCreateDataSourceOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bedrock_create_data_source]
    :end-before: [END howto_operator_bedrock_create_data_source]

.. _howto_operator:BedrockDeleteDataSource:

Delete an Amazon Bedrock Data Source
====================================

Deleting a Data Source is a simple boto API call and can be done in a TaskFlow task like the example below.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :start-after: [START howto_operator_bedrock_delete_data_source]
    :end-before: [END howto_operator_bedrock_delete_data_source]

.. _howto/operator:BedrockIngestDataOperator:

Ingest data into an Amazon Bedrock Data Source
==============================================

To add data from an Amazon S3 bucket into an Amazon Bedrock Data Source, you can use
:class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockIngestDataOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bedrock_ingest_data]
    :end-before: [END howto_operator_bedrock_ingest_data]

.. _howto/operator:BedrockRetrieveOperator:

Amazon Bedrock Retrieve
=======================

To query a knowledge base, you can use :class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockRetrieveOperator`.

The response will only contain citations to sources that are relevant to the query.  If you
would like to pass the results through an LLM in order to generate a text response, see
:class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockRaGOperator`

For more information on which models support retrieving information from a knowledge base, see
https://docs.aws.amazon.com/bedrock/latest/userguide/knowledge-base-supported.html

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bedrock_retrieve]
    :end-before: [END howto_operator_bedrock_retrieve]

.. _howto/operator:BedrockRaGOperator:

Amazon Bedrock Retrieve and Generate (RaG)
==========================================

To query a knowledge base or external sources and generate a text response based on the retrieved
results, you can use :class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockRaGOperator`.

The response will contain citations to sources that are relevant to the query as well as a generated text reply.
For more information on which models support retrieving information from a knowledge base, see
https://docs.aws.amazon.com/bedrock/latest/userguide/knowledge-base-supported.html

NOTE:  Support for "external sources" was added in boto 1.34.90

Example using an Amazon Bedrock Knowledge Base:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bedrock_knowledge_base_rag]
    :end-before: [END howto_operator_bedrock_knowledge_base_rag]

Example using a PDF file in an Amazon S3 Bucket:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bedrock_external_sources_rag]
    :end-before: [END howto_operator_bedrock_external_sources_rag]

.. _howto/operator:BedrockBatchInferenceOperator:

Create an Amazon Bedrock Batch Inference Job
============================================

To creates a batch inference job to invoke a model on multiple prompts, you can use
:class:`~airflow.providers.amazon.aws.operators.bedrock.BedrockBatchInferenceOperator`.

The input must be formatted in jsonl and uploaded to an Amazon S3 bucket.  Please see
https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference.html for details.

NOTE: Jobs are added to a queue and processed in order.  Given the potential wait times,
and the fact that the optional timeout parameter is measured in hours, deferrable mode is
recommended over "wait_for_completion" in this case.

Example using an Amazon Bedrock Batch Inference Job:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_batch_inference.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bedrock_batch_inference]
    :end-before: [END howto_operator_bedrock_batch_inference]


Sensors
-------

.. _howto/sensor:BedrockCustomizeModelCompletedSensor:

Wait for an Amazon Bedrock customize model job
==============================================

To wait on the state of an Amazon Bedrock customize model job until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.sensors.bedrock.BedrockCustomizeModelCompletedSensor`

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_customize_model]
    :end-before: [END howto_sensor_customize_model]

.. _howto/sensor:BedrockProvisionModelThroughputCompletedSensor:

Wait for an Amazon Bedrock provision model throughput job
=========================================================

To wait on the state of an Amazon Bedrock provision model throughput job until it reaches a
terminal state you can use
:class:`~airflow.providers.amazon.aws.sensors.bedrock.BedrockProvisionModelThroughputCompletedSensor`

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_provision_throughput]
    :end-before: [END howto_sensor_provision_throughput]

.. _howto/sensor:BedrockKnowledgeBaseActiveSensor:

Wait for an Amazon Bedrock Knowledge Base
=========================================

To wait on the state of an Amazon Bedrock Knowledge Base until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.sensors.bedrock.BedrockKnowledgeBaseActiveSensor`

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_bedrock_knowledge_base_active]
    :end-before: [END howto_sensor_bedrock_knowledge_base_active]

.. _howto/sensor:BedrockIngestionJobSensor:

Wait for an Amazon Bedrock ingestion job to finish
==================================================

To wait on the state of an Amazon Bedrock data ingestion job until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.sensors.bedrock.BedrockIngestionJobSensor`

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_retrieve_and_generate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_bedrock_ingest_data]
    :end-before: [END howto_sensor_bedrock_ingest_data]

.. _howto/sensor:BedrockBatchInferenceSensor:

Wait for an Amazon Bedrock batch inference job
==============================================

To wait on the state of an Amazon Bedrock batch inference job until it reaches the "Scheduled" or "Completed"
state you can use :class:`~airflow.providers.amazon.aws.sensors.bedrock.BedrockBatchInferenceScheduledSensor`

Bedrock adds batch inference jobs to a queue, and they may take some time to complete.  If you want to wait
for the job to complete, use TargetState.COMPLETED for the success_state, but if you only want to wait until
the service confirms that the job is in the queue, use TargetState.SCHEDULED.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_bedrock_batch_inference.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_bedrock_batch_inference_scheduled]
    :end-before: [END howto_sensor_bedrock_batch_inference_scheduled]

Reference
---------

* `AWS boto3 library documentation for Amazon Bedrock <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bedrock.html>`__
* `AWS boto3 library documentation for Amazon Bedrock Runtime <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bedrock-runtime.html>`__
* `AWS boto3 library documentation for Amazon Bedrock Agents <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bedrock-agent.html>`__
