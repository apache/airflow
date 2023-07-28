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

Google Cloud Data Pipelines Operators
=====================================

Data Pipelines is a Dataflow feature that allows customers to create 
and schedule recurring jobs, view aggregated job metrics, and define 
and manage job SLOs. A pipeline consists of a collection of jobs 
including ways to manage them. A pipeline may be associated with a 
Dataflow Template (classic/flex) and include all jobs launched with 
the associated template.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

Running a Data Pipeline
^^^^^^^^^^^^^^^^^^^^^^^

To run a Data Pipelines instance, use :class:`~airflow.providers.google.cloud.operators.datapipeline.RunDataPipelineOperator`. 
The operator accesses Google Cloud's Data Pipelines API and calls upon the 
`run method <https://cloud.google.com/dataflow/docs/reference/data-pipelines/rest/v1/projects.locations.pipelines/run>`__
to run the given pipeline.

:class:`~airflow.providers.google.cloud.operators.datapipeline.RunDataPipelineOperator` can take in four parameters:

- ``data_pipeline_name``: the name of the Data Pipelines instance
- ``project_id``: the ID of the GCP project that owns the job
- ``location``: the location of the Data Pipelines instance
- ``gcp_conn_id``: the connection ID to connect to the Google Cloud Platform

Only the Data Pipeline name and Project ID are required parameters, as the Location and GCP Connection ID have default values. 
The Project ID and Location will be used to build the parent name, which is where the given Data Pipeline should be located.

You can run a Data Pipelines instance by running the above parameters with RunDataPipelineOperator:
.. exampleinclude:: /../../providers/google/cloud/example_dags/google-datapipeline.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_run_data_pipeline]
    :end-before: [END howto_operator_run_data_pipeline]

Once called, the RunDataPipelineOperator will return the Google Cloud `Dataflow Job<https://cloud.google.com/dataflow/docs/reference/data-pipelines/rest/v1/Job>` 
created by running the given pipeline.
