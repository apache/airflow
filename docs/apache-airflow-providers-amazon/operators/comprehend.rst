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

=================
Amazon Comprehend
=================

`Amazon Comprehend <https://aws.amazon.com/comprehend/>`__ uses natural language processing (NLP) to
extract insights about the content of documents. It develops insights by recognizing the entities, key phrases,
language, sentiments, and other common elements in a document.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:ComprehendStartPiiEntitiesDetectionJobOperator:

Create an Amazon Comprehend Start PII Entities Detection Job
============================================================

To create an Amazon Comprehend Start PII Entities Detection Job, you can use
:class:`~airflow.providers.amazon.aws.operators.comprehend.ComprehendStartPiiEntitiesDetectionJobOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_comprehend.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_pii_entities_detection_job]
    :end-before: [END howto_operator_start_pii_entities_detection_job]

.. _howto/operator:ComprehendCreateDocumentClassifierOperator:

Create an Amazon Comprehend Document Classifier
===============================================

To create an Amazon Comprehend Document Classifier, you can use
:class:`~airflow.providers.amazon.aws.operators.comprehend.ComprehendCreateDocumentClassifierOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_comprehend_document_classifier.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_document_classifier]
    :end-before: [END howto_operator_create_document_classifier]

Sensors
-------

.. _howto/sensor:ComprehendStartPiiEntitiesDetectionJobCompletedSensor:

Wait for an Amazon Comprehend Start PII Entities Detection Job
==============================================================

To wait on the state of an Amazon Comprehend Start PII Entities Detection Job until it reaches a terminal
state you can use
:class:`~airflow.providers.amazon.aws.sensors.comprehend.ComprehendStartPiiEntitiesDetectionJobCompletedSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_comprehend.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_start_pii_entities_detection_job]
    :end-before: [END howto_sensor_start_pii_entities_detection_job]

.. _howto/sensor:ComprehendCreateDocumentClassifierCompletedSensor:

Wait for an Amazon Comprehend Document Classifier
==================================================

To wait on the state of an Amazon Comprehend Document Classifier until it reaches a terminal
state you can use
:class:`~airflow.providers.amazon.aws.sensors.comprehend.ComprehendCreateDocumentClassifierCompletedSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_comprehend_document_classifier.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_create_document_classifier]
    :end-before: [END howto_sensor_create_document_classifier]

Reference
---------

* `AWS boto3 library documentation for Amazon Comprehend <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/comprehend.html>`__
