..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
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

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst


.. _howto/operator:CloudAutoMLDocuments:

Creating Datasets
^^^^^^^^^^^^^^^^^

To create a Google AutoML dataset you can use
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLCreateDatasetOperator`.
The operator returns dataset id in :ref:`XCom <concepts:xcom>` under ``dataset_id`` key.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_automl_create_dataset]
    :end-before: [END howto_operator_automl_create_dataset]

After creating a dataset you can use it to import some data using
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLImportDataOperator`.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_automl_import_data]
    :end-before: [END howto_operator_automl_import_data]

To update dataset you can use
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLTablesUpdateDatasetOperator`.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_automl_update_dataset]
    :end-before: [END howto_operator_automl_update_dataset]

Listing Table And Columns Specs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To list table specs you can use
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLTablesListTableSpecsOperator`.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_automl_specs]
    :end-before: [END howto_operator_automl_specs]

To list column specs you can use
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLTablesListColumnSpecsOperator`.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_automl_column_specs]
    :end-before: [END howto_operator_automl_column_specs]

Operations On Models
^^^^^^^^^^^^^^^^^^^^

To create a Google AutoML model you can use
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLTrainModelOperator`.
The operator will wait for the operation to complete. Additionally the operator
returns the id of model in :ref:`XCom <concepts:xcom>` under ``model_id`` key.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_automl_create_model]
    :end-before: [END howto_operator_automl_create_model]

To get existing model one can use
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLGetModelOperator`.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_get_model]
    :end-before: [END howto_operator_get_model]

Once a model is created it could be deployed using
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLDeployModelOperator`.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_deploy_model]
    :end-before: [END howto_operator_deploy_model]

If you wish to delete a model you can use
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLDeleteModelOperator`.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_automl_delete_model]
    :end-before: [END howto_operator_automl_delete_model]

Making Predictions
^^^^^^^^^^^^^^^^^^

To obtain predictions from Google Cloud AutoML model you can use
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLPredictOperator` or
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLBatchPredictOperator`. In the first case
the model must be deployed.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_prediction]
    :end-before: [END howto_operator_prediction]

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_batch_prediction]
    :end-before: [END howto_operator_batch_prediction]

Listing And Deleting Datasets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can get a list of AutoML models using
:class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLDatasetListOperator`. The operator returns list
of datasets ids in :ref:`XCom <concepts:xcom>` under ``dataset_id_list`` key.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_list_dataset]
    :end-before: [END howto_operator_list_dataset]

To delete a model you can use :class:`~airflow.contrib.operators.gcp_automl_operator.AutoMLDatasetDeleteOperator`.
The delete operator allows also to pass list or coma separated string of datasets ids to be deleted.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_automl_operators.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_dataset]
    :end-before: [END howto_operator_delete_dataset]
