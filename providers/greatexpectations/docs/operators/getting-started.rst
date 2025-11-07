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

Getting started
===============

`Great Expectations <https://greatexpectations.io/>`__ (GX) is a framework for describing data using expressive tests
and then validating that the data meets test criteria. The Great Expectations Airflow Provider gives users a convenient
method for running validations directly from their DAGs. The Great Expectations Airflow Provider has three Operators
to choose from, which vary in the amount of configuration they require and the flexibility they provide.

* ``GXValidateDataFrameOperator``
* ``GXValidateBatchOperator``
* ``GXValidateCheckpointOperator``

Operator use cases
------------------

When deciding which Operator best fits your use case, consider the location of the data you are validating,
whether or not you need external alerts or actions to be triggered by the Operator, and what Data Context you will use.
When picking a Data Context, consider whether or not you need to view how results change over time.

* If your data is in memory as a Spark or Pandas DataFrame, we recommend using the ``GXValidateDataFrameOperator``.
  This option requires only a DataFrame and your Expectations to create a validation result.
* If your data is not in memory, we recommend configuring GX to connect to it by defining a BatchDefinition with the
  ``GXValidateBatchOperator``. This option requires a BatchDefinition and your Expectations to create a validation result.
* If you want to `trigger actions <https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions>`__
  based on validation results, use the ``GXValidateCheckpointOperator``. This option supports all features of GX Core,
  so it requires the most configuration - you have to define a Checkpoint, BatchDefinition, ExpectationSuite, and
  ValidationDefinition to get validation results.

The Operators vary in which `Data Contexts <https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context>`__
they support. All 3 Operators support Ephemeral and GX Cloud Data Contexts. Only the ``GXValidateCheckpointOperator``
supports the File Data Context.

* If the results are used only within the Airflow DAG by other tasks, we recommend using an Ephemeral Data Context.
  The serialized Validation Result will be available within the DAG as the task result, but will not persist externally
  for viewing the results across multiple runs. All 3 Operators support the Ephemeral Data Context.
* To persist and view results outside of Airflow, we recommend using a Cloud Data Context. Validation Results are
  automatically visible in the GX Cloud UI when using a Cloud Data Context, and the task result contains a link to
  the stored validation result. All 3 Operators support the Cloud Data Context.
* If you want to manage Validation Results yourself, use a File Data Context. With this option, Validation Results
  can be viewed in `Data Docs <https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_data_docs/>`__.
  Only the ``GXValidateCheckpointOperator`` supports the File Data Context.

Prerequisites
-------------

* `Python <https://www.python.org/>`__ version 3.9 to 3.12
* `Great Expectations <https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/install_gx>`__ version 1.3.11+
* `Apache Airflow <https://airflow.apache.org/>`__ version 2.1.0+

Assumed knowledge
-----------------

To get the most out of this getting started guide, make sure you have an understanding of:

* The basics of Great Expectations. See `Try GX Core <https://docs.greatexpectations.io/docs/core/introduction/try_gx/>`__.
* Airflow fundamentals, such as writing DAGs and defining tasks. See `Get started with Apache Airflow <https://www.astronomer.io/docs/learn/get-started-with-airflow/>`__.
* Airflow Operators. See `Operators 101 <https://www.astronomer.io/docs/learn/what-is-an-operator/>`__.
* Airflow connections. See `Managing your Connections in Apache Airflow <https://www.astronomer.io/docs/learn/connections/>`__.

Install the provider and dependencies
--------------------------------------

1. Install the provider.

   .. code-block:: bash

      pip install apache-airflow-providers-greatexpectations

2. (Optional) Install additional dependencies for the data sources you'll use. For example, to install the optional
   Snowflake dependency, use the following command.

   .. code-block:: bash

      pip install apache-airflow-providers-greatexpectations[snowflake]

   The following backends are supported as optional dependencies:

   * ``athena``
   * ``azure``
   * ``bigquery``
   * ``gcp``
   * ``mssql``
   * ``postgresql``
   * ``s3``
   * ``snowflake``
   * ``spark``
