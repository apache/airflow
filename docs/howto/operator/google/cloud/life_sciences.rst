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

.. contents::
  :depth: 1
  :local:


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst


Pipeline Configuration
^^^^^^^^^^^^^^^^^^^^^^
In order to run the pipeline, it is necessary to configure the request body.
Here is an example of the pipeline configuration with a single action.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_life_sciences.py
    :language: python
    :dedent: 0
    :start-after: [START howto_configure_simple_action_pipeline]
    :end-before: [END howto_configure_simple_action_pipeline]

The pipeline can also be configured with multiple action.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_life_sciences.py
    :language: python
    :dedent: 0
    :start-after: [START howto_configure_multiple_action_pipeline]
    :end-before: [END howto_configure_multiple_action_pipeline]

Read about the `request body parameters <https://cloud.google.com/life-sciences/docs/reference/rest/v2beta/projects.locations.pipelines/run?authuser=1#request-body/>`__
to understand all the fields you can include in the configuration

.. _howto/operator:LifeSciencesRunPipelineOperator:

Running a pipeline
-------------------------------
Use the
:class:`~airflow.providers.google.cloud.operators.life_sciences.LifeSciencesRunPipelineOperator`
to execute pipelines.

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_life_sciences.py
    :language: python
    :dedent: 0
    :start-after: [START howto_run_pipeline]
    :end-before: [END howto_run_pipeline]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://cloud.google.com/life-sciences/docs/reference/rest>`__
* `Product Documentation <https://cloud.google.com/life-sciences/docs/>`__
