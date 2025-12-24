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

Ray Job Operators
=================

The Ray Job operators provide a high-level interface for interacting with remote Ray clusters
using the Ray Jobs API. These operators can be used with clusters running on Google Cloud Vertex AI Ray,
GKE (self-managed Ray clusters) or any Ray cluster reachable through a dashboard address or Ray Client address.

The operators allow you to submit jobs, monitor their progress, retrieve logs,
and manage job lifecycle from Airflow.

Submitting Ray Jobs
^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.google.cloud.operators.ray.RaySubmitJobOperator`
submits a job to a Ray cluster and optionally waits for completion.

It supports waiting for job completion with ``wait_for_job_done``
and retrieving logs after completion with ``get_job_logs`` parameters.

.. exampleinclude:: /../../google/tests/system/google/cloud/ray/example_ray_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_ray_submit_job]
    :end-before: [END how_to_ray_submit_job]

Stopping Ray Jobs
^^^^^^^^^^^^^^^^^

Use :class:`~airflow.providers.google.cloud.operators.ray.RayStopJobOperator`
to stop a running Ray job identified by its job ID.

.. exampleinclude:: /../../google/tests/system/google/cloud/ray/example_ray_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_ray_stop_job]
    :end-before: [END how_to_ray_stop_job]

Deleting Ray Jobs
^^^^^^^^^^^^^^^^^

Use :class:`~airflow.providers.google.cloud.operators.ray.RayDeleteJobOperator`
to delete a job and its metadata after it reaches a terminal state.

.. exampleinclude:: /../../google/tests/system/google/cloud/ray/example_ray_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_ray_delete_job]
    :end-before: [END how_to_ray_delete_job]

Retrieving Job Information
^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.google.cloud.operators.ray.RayGetJobInfoOperator`
retrieves detailed information about a Ray job, including status, timestamps,
entrypoint, metadata, and runtime environment.

.. exampleinclude:: /../../google/tests/system/google/cloud/ray/example_ray_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_ray_get_job_info]
    :end-before: [END how_to_ray_get_job_info]

Listing Jobs
^^^^^^^^^^^^

Use :class:`~airflow.providers.google.cloud.operators.ray.RayListJobsOperator`
to list all jobs that have run on the cluster.

.. exampleinclude:: /../../google/tests/system/google/cloud/ray/example_ray_job.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_ray_list_jobs]
    :end-before: [END how_to_ray_list_jobs]
