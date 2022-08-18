
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

Apache Beam Operators
=====================

`Apache Beam <https://beam.apache.org/>`__ is an open source, unified model for defining both batch and
streaming data-parallel processing pipelines. Using one of the open source Beam SDKs, you build a program
that defines the pipeline. The pipeline is then executed by one of Beam's supported distributed processing
back-ends, which include Apache Flink, Apache Spark, and Google Cloud Dataflow.


.. _howto/operator:BeamRunPythonPipelineOperator:

Run Python Pipelines in Apache Beam
===================================

The ``py_file`` argument must be specified for
:class:`~airflow.providers.apache.beam.operators.beam.BeamRunPythonPipelineOperator`
as it contains the pipeline to be executed by Beam. The Python file can be available on GCS that Airflow
has the ability to download or available on the local filesystem (provide the absolute path to it).

The ``py_interpreter`` argument specifies the Python version to be used when executing the pipeline, the default
is ``python3`. If your Airflow instance is running on Python 2 - specify ``python2`` and ensure your ``py_file`` is
in Python 2. For best results, use Python 3.

If ``py_requirements`` argument is specified a temporary Python virtual environment with specified requirements will be created
and within it pipeline will run.

The ``py_system_site_packages`` argument specifies whether or not all the Python packages from your Airflow instance,
will be accessible within virtual environment (if ``py_requirements`` argument is specified),
recommend avoiding unless the Dataflow job requires it.

Python Pipelines with DirectRunner
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_python.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_python_direct_runner_pipeline_local_file]
    :end-before: [END howto_operator_start_python_direct_runner_pipeline_local_file]

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_python.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_python_direct_runner_pipeline_gcs_file]
    :end-before: [END howto_operator_start_python_direct_runner_pipeline_gcs_file]

Python Pipelines with DataflowRunner
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_python.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_python_dataflow_runner_pipeline_gcs_file]
    :end-before: [END howto_operator_start_python_dataflow_runner_pipeline_gcs_file]

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_python_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_python_dataflow_runner_pipeline_async_gcs_file]
    :end-before: [END howto_operator_start_python_dataflow_runner_pipeline_async_gcs_file]

|
|

.. _howto/operator:BeamRunJavaPipelineOperator:

Run Java Pipelines in Apache Beam
=================================

For Java pipeline the ``jar`` argument must be specified for
:class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator`
as it contains the pipeline to be executed by Apache Beam. The JAR can be available on GCS that Airflow
has the ability to download or available on the local filesystem (provide the absolute path to it).

Java Pipelines with DirectRunner
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_beam.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_java_direct_runner_pipeline]
    :end-before: [END howto_operator_start_java_direct_runner_pipeline

Java Pipelines with DataflowRunner
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_java_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_java_dataflow_runner_pipeline]
    :end-before: [END howto_operator_start_java_dataflow_runner_pipeline

|
|

.. _howto/operator:BeamRunGoPipelineOperator:

Run Go Pipelines in Apache Beam
===============================

The ``go_file`` argument must be specified for
:class:`~airflow.providers.apache.beam.operators.beam.BeamRunGoPipelineOperator`
as it contains the pipeline to be executed by Beam. The Go file can be available on GCS that Airflow
has the ability to download or available on the local filesystem (provide the absolute path to it). When running
from the local filesystem the equivalent will be ``go run <go_file>``. If pulling from GCS bucket, beforehand it will
init the module and install dependencies with ``go run init example.com/main`` and ``go mod tidy``.

Go Pipelines with DirectRunner
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_go.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_go_direct_runner_pipeline_local_file]
    :end-before: [END howto_operator_start_go_direct_runner_pipeline_local_file]

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_go.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_go_direct_runner_pipeline_gcs_file]
    :end-before: [END howto_operator_start_go_direct_runner_pipeline_gcs_file]

Go Pipelines with DataflowRunner
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_go.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_go_dataflow_runner_pipeline_gcs_file]
    :end-before: [END howto_operator_start_go_dataflow_runner_pipeline_gcs_file]

.. exampleinclude:: /../../tests/system/providers/apache/beam/example_go_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_go_dataflow_runner_pipeline_async_gcs_file]
    :end-before: [END howto_operator_start_go_dataflow_runner_pipeline_async_gcs_file]

Reference
^^^^^^^^^

For further information, look at:

* `Apache Beam Documentation <https://beam.apache.org/documentation/>`__
* `Google Cloud API Documentation <https://cloud.google.com/dataflow/docs/apis>`__
* `Product Documentation <https://cloud.google.com/dataflow/docs/>`__
* `Dataflow Monitoring Interface <https://cloud.google.com/dataflow/docs/guides/using-monitoring-intf/>`__
* `Dataflow Command-line Interface <https://cloud.google.com/dataflow/docs/guides/using-command-line-intf/>`__
