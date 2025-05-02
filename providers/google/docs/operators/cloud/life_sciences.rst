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



Google Cloud Life Sciences Operators
====================================
The `Google Cloud Life Sciences <https://cloud.google.com/life-sciences/>`__ is a service that executes
series of compute engine containers on the Google Cloud. It is used to process, analyze and annotate genomics
and biomedical data at scale.

.. warning::
    The Cloud Life Sciences will be discontinued on July 8, 2025. Please, use Google Cloud Batch instead.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:LifeSciencesRunPipelineOperator:

Running a pipeline
-------------------------------
Use the
:class:`~airflow.providers.google.cloud.operators.life_sciences.LifeSciencesRunPipelineOperator`
to execute pipelines.

This operator is deprecated and will be removed after July 08, 2025.
All the functionality and new features are available on the Google Cloud Batch platform. Please use
:class:`~airflow.providers.google.cloud.operators.CloudBatchSubmitJobOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/cloud_batch/example_cloud_batch.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_batch_job_creation]
    :end-before: [END howto_operator_batch_job_creation]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/life-sciences/docs/reference/rest>`__
* `Product Documentation <https://cloud.google.com/life-sciences/docs/>`__
