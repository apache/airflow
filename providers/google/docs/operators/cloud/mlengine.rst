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



Google Cloud AI Platform Operators
==================================

`Google Cloud AI Platform <https://cloud.google.com/ai-platform/>`__ (formerly known
as ML Engine) can be used to train machine learning models at scale, host trained models
in the cloud, and use models to make predictions for new data. AI Platform is a collection
of tools for training, evaluating, and tuning machine learning models. AI Platform can also
be used to deploy a trained model, make predictions, and manage various model versions.

The legacy versions of AI Platform Training, AI Platform Prediction, AI Platform Pipelines,
and AI Platform Data Labeling Service are deprecated and will no longer be available on
Google Cloud after their shutdown date. All the functionality of legacy AI Platform and new
features are available on the Vertex AI platform.

Prerequisite tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

Launching a Job
^^^^^^^^^^^^^^^
This function is deprecated. All the functionality of legacy MLEngine and new features are available on
the Vertex AI platform.

.. _howto/operator:MLEngineCreateModelOperator:

Creating a model
^^^^^^^^^^^^^^^^
A model is a container that can hold multiple model versions. A new model can be created through the
:class:`~airflow.providers.google.cloud.operators.mlengine.MLEngineCreateModelOperator`.
The ``model`` field should be defined with a dictionary containing the information about the model.
``name`` is a required field in this dictionary.

.. warning::
    This operator is deprecated. The model is created as a result of running Vertex AI operators that create training jobs
    of any types. For example, you can use
    :class:`~airflow.providers.google.cloud.operators.vertex_ai.custom_job.CreateCustomPythonPackageTrainingJobOperator`.
    The result of running this operator will be ready-to-use model saved in Model Registry.

.. exampleinclude:: /../../google/tests/system/google/cloud/ml_engine/example_mlengine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_custom_python_training_job_v1]
    :end-before: [END howto_operator_create_custom_python_training_job_v1]

Getting a model
^^^^^^^^^^^^^^^
This function is deprecated. All the functionality of legacy MLEngine and new features are available on
the Vertex AI platform.

Creating model versions
^^^^^^^^^^^^^^^^^^^^^^^

This function is deprecated. All the functionality of legacy MLEngine and new features are available on
the Vertex AI platform.
For model versioning please check:
`Model versioning with Vertex AI
<https://cloud.google.com/vertex-ai/docs/model-registry/versioning>`__

Managing model versions
^^^^^^^^^^^^^^^^^^^^^^^

This function is deprecated. All the functionality of legacy MLEngine and new features are available on
the Vertex AI platform.
For model versioning please check:
`Model versioning with Vertex AI
<https://cloud.google.com/vertex-ai/docs/model-registry/versioning>`__

Making predictions
^^^^^^^^^^^^^^^^^^

This function is deprecated. All the functionality of legacy MLEngine and new features are available on
the Vertex AI platform.


Cleaning up
^^^^^^^^^^^

This function is deprecated. All the functionality of legacy MLEngine and new features are available on
the Vertex AI platform.

Evaluating a model
^^^^^^^^^^^^^^^^^^

This function is deprecated. All the functionality of legacy MLEngine and new features are available on
the Vertex AI platform.
To create and view Model Evaluation, please check the documentation:
`Evaluate models using Vertex AI
<https://cloud.google.com/vertex-ai/docs/evaluation/using-model-evaluation#create_an_evaluation>`__

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/ai-platform/prediction/docs/reference/rest>`__
* `Product Documentation <https://cloud.google.com/ai-platform/docs/>`__
