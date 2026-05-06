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

===============================
Amazon SageMaker Unified Studio
===============================

`Amazon SageMaker Unified Studio <https://aws.amazon.com/sagemaker/unified-studio/>`__ is a unified development experience that
brings together AWS data, analytics, artificial intelligence (AI), and machine learning (ML) services.
It provides a place to build, deploy, execute, and monitor end-to-end workflows from a single interface.
This helps drive collaboration across teams and facilitate agile development.

Airflow provides different operators for running artifacts in SageMaker Unified Studio. Read the descriptions
below to understand which operator is best suited for your use case.

Prerequisite Tasks
------------------

To use these operators, you must do a few things:

  * Create a SageMaker Unified Studio domain and project, following the instruction in `AWS documentation <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/getting-started.html>`__.
  * If the domain is an IdC domain, navigate to the "Compute > Workflow environments" tab, and click "Create" to create a new MWAA environment.
  * Create a Jupyter notebook, querybook, Visual ETL job, or SageMaker Unified Studio notebook and save it to your project.

Operators
---------

.. _howto/operator:SageMakerNotebookOperator:

Run Jupyter notebooks, Querybooks, and Visual ETL jobs
======================================================

Use :class:`~airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookOperator`
to execute Jupyter notebooks, querybooks, and visual ETL jobs. This operator relies on the ``sagemaker_studio``
Python library to execute these artifacts.

The artifact is identified by its relative file path within the project (e.g. ``test_notebook.ipynb``).

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_unified_studio.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_unified_studio_notebook]
    :end-before: [END howto_operator_sagemaker_unified_studio_notebook]

.. _howto/operator:SageMakerUnifiedStudioNotebookOperator:

Run SageMaker Unified Studio notebooks
===============================================

Use :class:`~airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook.SageMakerUnifiedStudioNotebookOperator`
to execute SageMaker Unified Studio notebooks through the DataZone ``StartNotebookRun`` API.

The notebook is identified by its notebook ID (e.g. ``nb-1234567890``), along with the domain ID and project ID
where the notebook resides.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_unified_studio_notebook.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_unified_studio_notebook]
    :end-before: [END howto_operator_sagemaker_unified_studio_notebook]


The following example adds domain ID, project ID, and domain name as operator parameters.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_unified_studio.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_unified_studio_notebook_explicit_params]
    :end-before: [END howto_operator_sagemaker_unified_studio_notebook_explicit_params]

Notebooks can produce output variables that are automatically pushed to XCom when the run completes.
Downstream tasks can consume these outputs via Jinja templating in ``notebook_parameters``.

In this example, Notebook A produces outputs (e.g., ``name`` and ``age``). Notebook B receives
those values as parameters using Jinja templates like
``{{ task_instance.xcom_pull(task_ids='notebook-a-task', key='name') }}``.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_sagemaker_unified_studio_notebook.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_unified_studio_notebook_pass_outputs]
    :end-before: [END howto_operator_sagemaker_unified_studio_notebook_pass_outputs]

Reference
---------

* `What is Amazon SageMaker Unified Studio <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/what-is-sagemaker-unified-studio.html>`__
