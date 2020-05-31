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

Google Cloud Dataproc Operators
===============================

Dataproc is a managed Apache Spark and Apache Hadoop service that lets you
take advantage of open source data tools for batch processing, querying, streaming and machine learning.
Dataproc automation helps you create clusters quickly, manage them easily, and
save money by turning clusters off when you don't need them.

For more information about the service visit `Dataproc production documentation <Product documentation <https://cloud.google.com/dataproc/docs/reference>`__

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst


.. _howto/operator:DataprocCreateClusterOperator:

Create a Cluster
----------------

Before you create a dataproc cluster you need to define the cluster.
It describes the identifying information, config, and status of a cluster of Compute Engine instances.
For more information about the available fields to pass when creating a cluster, visit `Dataproc create cluster API. <https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#Cluster>`__

A cluster configuration can look as followed:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_create_cluster]
    :end-before: [END how_to_cloud_dataproc_create_cluster]

With this configuration we can create the cluster:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_create_cluster_operator]

Update a cluster
----------------
You can scale the cluster up or down by providing a cluster config and a updateMask.
In the updateMask argument you specifies the path, relative to Cluster, of the field to update.
For more information on updateMask and other parameters take a look at `Dataproc update cluster API. <https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/patch>`__

An example of a new cluster config and the updateMask:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_updatemask_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_updatemask_cluster_operator]

To update a cluster you can use:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocUpdateClusterOperator`

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_update_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_update_cluster_operator]

Deleting a cluster
------------------

To delete a cluster you can use:

:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_delete_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_delete_cluster_operator]

Submit a job to a cluster
-------------------------

Dataproc supports submitting jobs of different big data components.
The list currently includes Spark, Hadoop, Pig and Hive.
For more information on versions and images take a look at `Cloud Dataproc Image version list <https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions>`__

To submit a job to the cluster you need a provide a job source file. The job source file can be on GCS, the cluster or on your local
file system. You can specify a file:/// path to refer to a local file on a cluster's master node.

The job configuration can be submitted by using:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_submit_job_to_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_submit_job_to_cluster_operator]

Examples of job configurations to submit
----------------------------------------

We have provided an example for every framework below.
There are more arguments to provide in the jobs than the examples show. For the complete list of arguments take a look at
`DataProc Job arguments <https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs>`__

Example of the configuration for a PySpark Job:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_pyspark_config]
    :end-before: [END how_to_cloud_dataproc_pyspark_config]

Example of the configuration for a SparkSQl Job:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_sparksql_config]
    :end-before: [END how_to_cloud_dataproc_sparksql_config]

Example of the configuration for a Spark Job:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_spark_config]
    :end-before: [END how_to_cloud_dataproc_spark_config]

Example of the configuration for a Hive Job:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_hive_config]
    :end-before: [END how_to_cloud_dataproc_hive_config]

Example of the configuration for a Hadoop Job:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_hadoop_config]
    :end-before: [END how_to_cloud_dataproc_hadoop_config]

Example of the configuration for a Pig Job:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_pig_config]
    :end-before: [END how_to_cloud_dataproc_pig_config]


Example of the configuration for a SparkR:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_sparkr_config]
    :end-before: [END how_to_cloud_dataproc_sparkr_config]

References
^^^^^^^^^^
For further information, take a look at:

* `DataProc API documentation <https://cloud.google.com/dataproc/docs/reference>`__
* `Product documentation <https://cloud.google.com/dataproc/docs/reference>`__
