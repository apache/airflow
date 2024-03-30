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

Google Cloud AutoML Operators
=======================================

The `Google Cloud AutoML <https://cloud.google.com/automl/docs/>`__
makes the power of machine learning available to you even if you have limited knowledge
of machine learning. You can use AutoML to build on Google's machine learning capabilities
to create your own custom machine learning models that are tailored to your business needs,
and then integrate those models into your applications and web sites.

As of March 31, 2024, GCP has shut down most of the AutoML domains (AutoML Tables, AutoML Video Intelligence,
AutoML Vision, and AutoML Natural Language) in favor of equivalent domains of Vertex AI, and retained only AutoML Translation.
All the functionality of legacy features are available on the Vertex AI operators - please refer to
:doc:`/operators/cloud/vertex_ai` for more information.
Please avoid using the AutoML operators for domains other than AutoML translation.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudAutoMLDocuments:
.. _howto/operator:AutoMLCreateDatasetOperator:
.. _howto/operator:AutoMLImportDataOperator:
.. _howto/operator:AutoMLUpdateDatasetOperator:

Creating Datasets
^^^^^^^^^^^^^^^^^

To create a Google AutoML dataset you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLCreateDatasetOperator`.
The operator returns dataset id in :ref:`XCom <concepts:xcom>` under ``dataset_id`` key.
After creating a dataset you can use it to import some data using
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLImportDataOperator`.
To update dataset you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLUpdateDatasetOperator`.


.. _howto/operator:AutoMLTrainModelOperator:
.. _howto/operator:AutoMLGetModelOperator:
.. _howto/operator:AutoMLDeleteModelOperator:

Operations On Models
^^^^^^^^^^^^^^^^^^^^

To create a Google AutoML model you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLTrainModelOperator`.
The operator will wait for the operation to complete. Additionally the operator
returns the id of model in :ref:`XCom <concepts:xcom>` under ``model_id`` key.

To get an existing model one can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLGetModelOperator`.

If you wish to delete a model you can use
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLDeleteModelOperator`.

.. _howto/operator:AutoMLPredictOperator:
.. _howto/operator:AutoMLBatchPredictOperator:
.. _howto/operator:AutoMLListDatasetOperator:
.. _howto/operator:AutoMLDeleteDatasetOperator:

Listing And Deleting Datasets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can get a list of AutoML datasets using
:class:`~airflow.providers.google.cloud.operators.automl.AutoMLListDatasetOperator`. The operator returns list
of datasets ids in :ref:`XCom <concepts:xcom>` under ``dataset_id_list`` key.

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/automl/index.html>`__
* `Product Documentation <https://cloud.google.com/automl/docs/>`__
