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

Google Cloud Dataflow Operators
===============================

`Dataflow <https://cloud.google.com/dataflow/>`__ is a managed service for
executing a wide variety of data processing patterns. These pipelines are created
using the Apache Beam programming model which allows for both batch and streaming.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst

Ways to run a data pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are multiple options to execute a Dataflow pipeline on Airflow. If looking to execute the pipeline
code from a source file (Java or Python) it would be best to use the language specific create operators.
If a process exists to stage the pipeline code in an abstracted manner - a Templated job would be best as
it allows development of the application without minimal intrusion to the DAG containing operators for it.

.. _howto/operator:DataflowCreateJavaJobOperator:
.. _howto/operator:DataflowCreatePythonJobOperator:

Starting a new job
""""""""""""""""""

To create a new pipeline using the source file (JAR in Java or Python file) use
the create job operators. The source file can be located on GCS or on the local filesystem.
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator`
or
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`

Please see the notes below on Java and Python specific SDKs as they each have their own set
of execution options when running pipelines.

Here is an example of creating and running a pipeline in Java:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_java_job]
    :end-before: [END howto_operator_start_java_job]

.. _howto/operator:DataflowTemplatedJobStartOperator:

Templated jobs
""""""""""""""

Templates give the ability to stage a pipeline on Cloud Storage and run it from there. This
provides flexibility in the development workflow as it separates the development of a pipeline
from the staging and execution steps. To start a templated job use the
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator`

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_template_job]
    :end-before: [END howto_operator_start_template_job]

See the `list of Google-provided templates that can be used with this operator
<https://cloud.google.com/dataflow/docs/guides/templates/provided-templates>`_.

Execution options for pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dataflow has multiple options of executing pipelines. It can be done in the following modes:
asynchronously (fire and forget), blocking (wait until completion), or streaming (run indefinitely).
In Airflow it is best to use asychronous pipelines as blocking ones tax the Airflow resources by listening
to the job until it completes.

Asynchronous execution
""""""""""""""""""""""

Dataflow jobs are by default asynchronous - however this is dependent on the application code (contained in the JAR
or Python file) and how it is written. In order for the Dataflow job to execute asynchronously, ensure the
pipeline objects are not being waited upon (not calling ``waitUntilFinish`` or ``wait_until_finish`` on the
``PipelineResult`` in your application code).

This is the recommended way to execute your pipelines when using Airflow.

Use the Dataflow monitoring or command-line interface to view the details of your pipeline's results after Airflow
runs the operator.

Blocking execution
""""""""""""""""""

In order for a Dataflow job to execute and wait until completion, ensure the pipeline objects are waited upon
in the application code. This can be done for the Java SDK by calling ``waitUntilFinish`` on the ``PipelineResult``
returned from ``pipeline.run()`` or for the Python SDK by calling ``wait_until_finish`` on the ``PipelineResult`` r
eturned from ``pipeline.run()``.

Blocking jobs should be avoided as there is a background process that occurs when run on Airflow. This process is
continuously being run to wait for the Dataflow job to be completed and increases the consumption of resources by
Airflow in doing so.

Streaming execution
"""""""""""""""""""

To execute a streaming Dataflow job, ensure the streaming option is set (for Python) or read from an unbounded data
source, such as Pub/Sub, in your pipeline (for Java).

The following example in Python demonstrates how to use the flag in the
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_streaming_python]
    :end-before: [END howto_operator_streaming_python]

Once a streaming job starts it will not stop until explicitly specified via a command on the Dataflow monitoring or
command-line interface.

Language specific pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Based on which language (SDK) is used for the Dataflow operators, there are specific options to be wary of.

Java SDK pipelines
""""""""""""""""""

The ``jar`` argument must be specified for
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator`
as it contains the pipeline to be executed on Dataflow. The JAR can be available on GCS that Airflow
has the ability to download or available on the local filesystem (provide the absolute path to it).

Python SDK pipelines
""""""""""""""""""""

The ``py_file`` argument must be specified for
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`
as it contains the pipeline to be executed on Dataflow. The Python file can be available on GCS that Airflow
has the ability to download or available on the local filesystem (provide the absolute path to it).

The ``py_interpreter`` argument specifies the Python version to be used when executing the pipeline, the default
is ``python3`. If your Airflow instance is running on Python 2 - specify ``python2`` and ensure your ``py_file`` is
in Python 2. For best results, use Python 3.

The ``py_requirements`` argument specifies the Python dependencies necessary to run your pipeline.

The ``py_system_site_packages`` argument specifies whether or not all the Python packages from your Airflow instance,
recommend avoiding unless the Dataflow job requires it.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_python_job]
    :end-before: [END howto_operator_start_python_job]

Reference
^^^^^^^^^

For further information, look at:

* `Google Cloud API Documentation <https://cloud.google.com/dataflow/docs/apis>`__
* `Apache Beam Documentation <https://beam.apache.org/documentation/>`__
* `Product Documentation <https://cloud.google.com/dataflow/docs/>`__
* `Dataflow Monitoring Interface <https://cloud.google.com/dataflow/docs/guides/using-monitoring-intf/>`__
* `Dataflow Command-line Interface <https://cloud.google.com/dataflow/docs/guides/using-command-line-intf/>`__
