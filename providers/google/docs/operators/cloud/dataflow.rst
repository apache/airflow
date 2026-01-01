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
using the Apache Beam programming model which allows for both batch and streaming processing.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

Ways to run a data pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are several ways to run a Dataflow pipeline depending on your environment, source files:

- **Non-templated pipeline**: Developer can run the pipeline as a local process on the Airflow worker
  if you have a ``*.jar`` file for Java or a ``*.py`` file for Python. This also means that the necessary system
  dependencies must be installed on the worker.  For Java, worker must have the JRE Runtime installed.
  For Python, the Python interpreter. The runtime versions must be compatible with the pipeline versions.
  This is the fastest way to start a pipeline, but because of its frequent problems with system dependencies,
  it may cause problems. See: :ref:`howto/operator:JavaSDKPipelines`,
  :ref:`howto/operator:PythonSDKPipelines` for more detailed information.
  Developer can also create a pipeline by passing its structure in a JSON format and then run it to create
  a Job.
  See: :ref:`howto/operator:DataflowCreatePipelineOperator` and :ref:`howto/operator:DataflowRunPipelineOperator`
  for more detailed information.
- **Templated pipeline**: The programmer can make the pipeline independent of the environment by preparing
  a template that will then be run on a machine managed by Google. This way, changes to the environment
  won't affect your pipeline. There are two types of the templates:

  - **Classic templates**. Developers run the pipeline and create a template. The Apache Beam SDK stages
    files in Cloud Storage, creates a template file (similar to job request),
    and saves the template file in Cloud Storage. See: :ref:`howto/operator:DataflowTemplatedJobStartOperator`
  - **Flex Templates**. Developers package the pipeline into a Docker image and then use the ``gcloud``
    command-line tool to build and save the Flex Template spec file in Cloud Storage. See:
    :ref:`howto/operator:DataflowStartFlexTemplateOperator`

It is a good idea to test your pipeline using the non-templated pipeline,
and then run the pipeline in production using the templates.

For details on the differences between the pipeline types, see
`Dataflow templates <https://cloud.google.com/dataflow/docs/concepts/dataflow-templates>`__
in the Google Cloud documentation.

Starting Non-templated pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _howto/operator:DataflowCreatePipelineOperator:
.. _howto/operator:DataflowRunPipelineOperator:

JSON-formatted pipelines
""""""""""""""""""""""""
A new pipeline can be created by passing its structure in a JSON format. See
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowCreatePipelineOperator`
This will create a new pipeline that will be visible on Dataflow Pipelines UI.

Here is an example of how you can create a Dataflow Pipeline by running DataflowCreatePipelineOperator:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_pipeline.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_create_dataflow_pipeline]
   :end-before: [END howto_operator_create_dataflow_pipeline]

To run a newly created pipeline you can use
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowRunPipelineOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_pipeline.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_run_dataflow_pipeline]
    :end-before: [END howto_operator_run_dataflow_pipeline]

Once called, the DataflowRunPipelineOperator will return the Google Cloud
`Dataflow Job <https://cloud.google.com/dataflow/docs/reference/data-pipelines/rest/v1/Job>`__
created by running the given pipeline.

Here is an streaming example of Dataflow Pipeline by running DataflowCreatePipelineOperator:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_pipeline_streaming.py
   :language: python
   :dedent: 4
   :start-after: [START howto_operator_create_dataflow_pipeline_streaming]
   :end-before: [END howto_operator_create_dataflow_pipeline_streaming]

Once pipeline created, the streaming job
the Google Cloud
`Dataflow Job <https://cloud.google.com/dataflow/docs/reference/data-pipelines/rest/v1/Job>`__
will start automatically.


For further information regarding the API usage, see
`Data Pipelines API REST Resource <https://cloud.google.com/dataflow/docs/reference/data-pipelines/rest/v1/projects.locations.pipelines#Pipeline>`__
in the Google Cloud documentation.

To create a new pipeline using the source file (JAR in Java or Python file) use
the create job operators. The source file can be located on GCS or on the local filesystem.
:class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator`
or
:class:`~airflow.providers.apache.beam.operators.beam.BeamRunPythonPipelineOperator`

.. _howto/operator:JavaSDKPipelines:

Java SDK pipelines
""""""""""""""""""

For Java pipeline the ``jar`` argument must be specified for
:class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator`
as it contains the pipeline to be executed on Dataflow. The JAR can be available on GCS that Airflow
has the ability to download or available on the local filesystem (provide the absolute path to it).

Here is an example of creating and running a pipeline in Java with jar stored on GCS:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_native_java.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_java_job_jar_on_gcs]
    :end-before: [END howto_operator_start_java_job_jar_on_gcs]

Here is an example of creating and running a pipeline in Java with jar stored on GCS in deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_native_java.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_java_job_jar_on_gcs_deferrable]
    :end-before: [END howto_operator_start_java_job_jar_on_gcs_deferrable]

Here is an example of creating and running a streaming pipeline in Java with jar stored on GCS:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_java_streaming.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_java_streaming]
    :end-before: [END howto_operator_start_java_streaming]

Here is an Java dataflow streaming pipeline example in deferrable_mode :

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_java_streaming.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_java_streaming_deferrable]
    :end-before: [END howto_operator_start_java_streaming_deferrable]


.. _howto/operator:PythonSDKPipelines:

Python SDK pipelines
""""""""""""""""""""

The ``py_file`` argument must be specified for
:class:`~airflow.providers.apache.beam.operators.beam.BeamRunPythonPipelineOperator`
as it contains the pipeline to be executed on Dataflow. The Python file can be available on GCS that Airflow
has the ability to download or available on the local filesystem (provide the absolute path to it).

The ``py_interpreter`` argument specifies the Python version to be used when executing the pipeline, the default
is ``python3``. If your Airflow instance is running on Python 2 - specify ``python2`` and ensure your ``py_file`` is
in Python 2. For best results, use Python 3.

If ``py_requirements`` argument is specified a temporary Python virtual environment with specified requirements will be created
and within it pipeline will run.

The ``py_system_site_packages`` argument specifies whether or not all the Python packages from your Airflow instance,
will be accessible within virtual environment (if ``py_requirements`` argument is specified),
recommend avoiding unless the Dataflow job requires it.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_native_python.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_python_job]
    :end-before: [END howto_operator_start_python_job]

Execution models
^^^^^^^^^^^^^^^^

Dataflow has multiple options of executing pipelines. It can be done in the following modes:
batch asynchronously (fire and forget), batch blocking (wait until completion), or streaming (run indefinitely).
In Airflow it is best practice to use asynchronous batch pipelines or streams and use sensors to listen for expected job state.

By default :class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator`,
:class:`~airflow.providers.apache.beam.operators.beam.BeamRunPythonPipelineOperator`,
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator` and
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowStartFlexTemplateOperator`
have argument ``wait_until_finished`` set to ``None`` which cause different behaviour depends on the type of pipeline:

* for the streaming pipeline, wait for jobs to start,
* for the batch pipeline, wait for the jobs to complete.

If ``wait_until_finished`` is set to ``True`` operator will always wait for end of pipeline execution.
If set to ``False`` only submits the jobs.

See: `Configuring PipelineOptions for execution on the Cloud Dataflow service <https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#configuring-pipelineoptions-for-execution-on-the-cloud-dataflow-service>`_

Asynchronous execution
""""""""""""""""""""""

Dataflow batch jobs are by default asynchronous; however, this is dependent on the application code (contained in the JAR
or Python file) and how it is written. In order for the Dataflow job to execute asynchronously, ensure the
pipeline objects are not being waited upon (not calling ``waitUntilFinish`` or ``wait_until_finish`` on the
``PipelineResult`` in your application code).

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_native_python_async.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_python_job_async]
    :end-before: [END howto_operator_start_python_job_async]

Blocking execution
""""""""""""""""""

In order for a Dataflow job to execute and wait until completion, ensure the pipeline objects are waited upon
in the application code. This can be done for the Java SDK by calling ``waitUntilFinish`` on the ``PipelineResult``
returned from ``pipeline.run()`` or for the Python SDK by calling ``wait_until_finish`` on the ``PipelineResult``
returned from ``pipeline.run()``.

Blocking jobs should be avoided as there is a background process that occurs when run on Airflow. This process is
continuously being run to wait for the Dataflow job to be completed and increases the consumption of resources by
Airflow in doing so.

Streaming execution
"""""""""""""""""""

To execute a streaming Dataflow job, ensure the streaming option is set (for Python) or read from an unbounded data
source, such as Pub/Sub, in your pipeline (for Java).

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_streaming_python.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_streaming_python_job]
    :end-before: [END howto_operator_start_streaming_python_job]

Deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_streaming_python.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_streaming_python_job_deferrable]
    :end-before: [END howto_operator_start_streaming_python_job_deferrable]


Setting argument ``drain_pipeline`` to ``True`` allows to stop streaming job by draining it
instead of canceling during killing task instance.

See the `Stopping a running pipeline
<https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline>`_.


.. _howto/operator:DataflowTemplatedJobStartOperator:
.. _howto/operator:DataflowStartFlexTemplateOperator:

Templated jobs
""""""""""""""

Templates give the ability to stage a pipeline on Cloud Storage and run it from there. This
provides flexibility in the development workflow as it separates the development of a pipeline
from the staging and execution steps. There are two types of templates for Dataflow: Classic and Flex.
See the `official documentation for Dataflow templates
<https://cloud.google.com/dataflow/docs/concepts/dataflow-templates>`_ for more information.

Here is an example of running a Dataflow job using a Classic Template with
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator`:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_template.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_template_job]
    :end-before: [END howto_operator_start_template_job]

Also for this action you can use the operator in the deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_template.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_template_job_deferrable]
    :end-before: [END howto_operator_start_template_job_deferrable]

See the `list of Google-provided templates that can be used with this operator
<https://cloud.google.com/dataflow/docs/guides/templates/provided-templates>`_.

Here is an example of running a Dataflow job using a Flex Template with
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowStartFlexTemplateOperator`:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_template.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_flex_template_job]
    :end-before: [END howto_operator_start_flex_template_job]

Also for this action you can use the operator in the deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_template.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_flex_template_job_deferrable]
    :end-before: [END howto_operator_start_flex_template_job_deferrable]

.. _howto/operator:DataflowStartYamlJobOperator:

Dataflow YAML
""""""""""""""
Beam YAML is a no-code SDK for configuring Apache Beam pipelines by using YAML files.
You can use Beam YAML to author and run a Beam pipeline without writing any code.
This API can be used to define both streaming and batch pipelines.

Here is an example of running Dataflow YAML job with
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowStartYamlJobOperator`:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_yaml.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataflow_start_yaml_job]
    :end-before: [END howto_operator_dataflow_start_yaml_job]

This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_yaml.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataflow_start_yaml_job_def]
    :end-before: [END howto_operator_dataflow_start_yaml_job_def]

.. warning::
    This operator requires ``gcloud`` command (Google Cloud SDK) must be installed on the Airflow worker
    <https://cloud.google.com/sdk/docs/install>`__

See the `Dataflow YAML reference
<https://cloud.google.com/sdk/gcloud/reference/dataflow/yaml>`_.

.. _howto/operator:DataflowStopJobOperator:

Stopping a pipeline
^^^^^^^^^^^^^^^^^^^
To stop one or more Dataflow pipelines you can use
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowStopJobOperator`.
Streaming pipelines are drained by default, setting ``drain_pipeline`` to ``False`` will cancel them instead.
Provide ``job_id`` to stop a specific job, or ``job_name_prefix`` to stop all jobs with provided name prefix.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_native_python.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_stop_dataflow_job]
    :end-before: [END howto_operator_stop_dataflow_job]

See: `Stopping a running pipeline
<https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline>`_.

.. _howto/operator:DataflowDeletePipelineOperator:

Deleting a pipeline
^^^^^^^^^^^^^^^^^^^
To delete a Dataflow pipeline you can use
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowDeletePipelineOperator`.
Here is an example how you can use this operator:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_pipeline.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_dataflow_pipeline]
    :end-before: [END howto_operator_delete_dataflow_pipeline]

Updating a pipeline
^^^^^^^^^^^^^^^^^^^
Once a streaming pipeline has been created and is running, its configuration cannot be changed because it is immutable. To make any modifications, you need to update the pipeline's definition (e.g., update your code or template), and then submit a new job.Essentially, you'll be creating a new instance of the pipeline with the desired updates.

For batch pipelines, if a job is currently running and you want to update its configuration, you must cancel the job. This is because once a Dataflow job has started, it becomes immutable. Although batch pipelines are designed to process a finite amount of data and will eventually be completed on their own, you cannot update a job that is in progress. If you need to change any parameters or the pipeline logic while the job is running, you will have to cancel the current run and then launch a new job with the updated configuration.

If the batch pipeline has already been completed successfully, then there is no running job to update; the new configuration will only be applied to the next job submission.

.. _howto/operator:DataflowJobStatusSensor:
.. _howto/operator:DataflowJobMetricsSensor:
.. _howto/operator:DataflowJobMessagesSensor:
.. _howto/operator:DataflowJobAutoScalingEventsSensor:

Sensors
^^^^^^^

When job is triggered asynchronously sensors may be used to run checks for specific job properties.

:class:`~airflow.providers.google.cloud.sensors.dataflow.DataflowJobStatusSensor`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_native_python_async.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_status]
    :end-before: [END howto_sensor_wait_for_job_status]

This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_sensors_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_status_deferrable]
    :end-before: [END howto_sensor_wait_for_job_status_deferrable]

:class:`~airflow.providers.google.cloud.sensors.dataflow.DataflowJobMetricsSensor`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_native_python_async.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_metric]
    :end-before: [END howto_sensor_wait_for_job_metric]

This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_sensors_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_metric_deferrable]
    :end-before: [END howto_sensor_wait_for_job_metric_deferrable]

:class:`~airflow.providers.google.cloud.sensors.dataflow.DataflowJobMessagesSensor`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_native_python_async.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_message]
    :end-before: [END howto_sensor_wait_for_job_message]

This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_sensors_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_message_deferrable]
    :end-before: [END howto_sensor_wait_for_job_message_deferrable]

:class:`~airflow.providers.google.cloud.sensors.dataflow.DataflowJobAutoScalingEventsSensor`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_native_python_async.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_autoscaling_event]
    :end-before: [END howto_sensor_wait_for_job_autoscaling_event]

This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataflow/example_dataflow_sensors_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wait_for_job_autoscaling_event_deferrable]
    :end-before: [END howto_sensor_wait_for_job_autoscaling_event_deferrable]

Reference
^^^^^^^^^

For further information, look at:

* `Google Cloud API Documentation <https://cloud.google.com/dataflow/docs/apis>`__
* `Apache Beam Documentation <https://beam.apache.org/documentation/>`__
* `Product Documentation <https://cloud.google.com/dataflow/docs/>`__
* `Dataflow Monitoring Interface <https://cloud.google.com/dataflow/docs/guides/using-monitoring-intf/>`__
* `Dataflow Command-line Interface <https://cloud.google.com/dataflow/docs/guides/using-command-line-intf/>`__
