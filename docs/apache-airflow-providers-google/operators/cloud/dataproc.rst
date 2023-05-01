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

Prerequisite Tasks
------------------

.. include::/operators/_partials/prerequisite_tasks.rst


.. _howto/operator:DataprocCreateClusterOperator:
.. _howto/operator:DataprocInstantiateInlineWorkflowTemplateOperator:

Create a Cluster
----------------

Before you create a dataproc cluster you need to define the cluster.
It describes the identifying information, config, and status of a cluster of Compute Engine instances.
For more information about the available fields to pass when creating a cluster, visit `Dataproc create cluster API. <https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#Cluster>`__

A cluster configuration can look as followed:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_hive.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_create_cluster]
    :end-before: [END how_to_cloud_dataproc_create_cluster]

With this configuration we can create the cluster:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_hive.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_create_cluster_operator]

For create Dataproc cluster in Google Kubernetes Engine you should use this cluster configuration:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_gke.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_create_cluster_in_gke_config]
    :end-before: [END how_to_cloud_dataproc_create_cluster_in_gke_config]

With this configuration we can create the cluster:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_gke.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_cluster_operator_in_gke]
    :end-before: [END how_to_cloud_dataproc_create_cluster_operator_in_gke]

You can use deferrable mode for this action in order to run the operator asynchronously:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_cluster_operator_async]
    :end-before: [END how_to_cloud_dataproc_create_cluster_operator_async]

Generating Cluster Config
^^^^^^^^^^^^^^^^^^^^^^^^^
You can also generate **CLUSTER_CONFIG** using functional API,
this could be easily done using **make()** of
:class:`~airflow.providers.google.cloud.operators.dataproc.ClusterGenerator`
You can generate and use config as followed:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_generator.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_create_cluster_generate_cluster_config]
    :end-before: [END how_to_cloud_dataproc_create_cluster_generate_cluster_config]

Update a cluster
----------------
You can scale the cluster up or down by providing a cluster config and a updateMask.
In the updateMask argument you specifies the path, relative to Cluster, of the field to update.
For more information on updateMask and other parameters take a look at `Dataproc update cluster API. <https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/patch>`__

An example of a new cluster config and the updateMask:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_update.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_updatemask_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_updatemask_cluster_operator]

To update a cluster you can use:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocUpdateClusterOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_update.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_update_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_update_cluster_operator]

You can use deferrable mode for this action in order to run the operator asynchronously:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_update_cluster_operator_async]
    :end-before: [END how_to_cloud_dataproc_update_cluster_operator_async]

Deleting a cluster
------------------

To delete a cluster you can use:

:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_hive.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_delete_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_delete_cluster_operator]

You can use deferrable mode for this action in order to run the operator asynchronously:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_delete_cluster_operator_async]
    :end-before: [END how_to_cloud_dataproc_delete_cluster_operator_async]

Submit a job to a cluster
-------------------------

Dataproc supports submitting jobs of different big data components.
The list currently includes Spark, Hadoop, Pig and Hive.
For more information on versions and images take a look at `Cloud Dataproc Image version list <https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions>`__

To submit a job to the cluster you need to provide a job source file. The job source file can be on GCS, the cluster or on your local
file system. You can specify a file:/// path to refer to a local file on a cluster's primary node.

The job configuration can be submitted by using:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_pyspark.py
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

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_pyspark.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_pyspark_config]
    :end-before: [END how_to_cloud_dataproc_pyspark_config]

Example of the configuration for a SparkSQl Job:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_spark_sql.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_sparksql_config]
    :end-before: [END how_to_cloud_dataproc_sparksql_config]

Example of the configuration for a Spark Job:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_spark.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_spark_config]
    :end-before: [END how_to_cloud_dataproc_spark_config]

Example of the configuration for a Spark Job running in `deferrable mode <https://airflow.apache.org/docs/apache-airflow/stable/concepts/deferring.html>`__:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_spark_deferrable.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_spark_deferrable_config]
    :end-before: [END how_to_cloud_dataproc_spark_deferrable_config]

Example of the configuration for a Hive Job:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_hive.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_hive_config]
    :end-before: [END how_to_cloud_dataproc_hive_config]

Example of the configuration for a Hadoop Job:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_hadoop.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_hadoop_config]
    :end-before: [END how_to_cloud_dataproc_hadoop_config]

Example of the configuration for a Pig Job:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_pig.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_pig_config]
    :end-before: [END how_to_cloud_dataproc_pig_config]


Example of the configuration for a SparkR:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_sparkr.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_sparkr_config]
    :end-before: [END how_to_cloud_dataproc_sparkr_config]

Working with workflows templates
--------------------------------

Dataproc supports creating workflow templates that can be triggered later on.

A workflow template can be created using:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocCreateWorkflowTemplateOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_workflow.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_workflow_template]
    :end-before: [END how_to_cloud_dataproc_create_workflow_template]

Once a workflow is created users can trigger it using
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateWorkflowTemplateOperator`:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_workflow.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_trigger_workflow_template]
    :end-before: [END how_to_cloud_dataproc_trigger_workflow_template]

Also for all this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_workflow_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_trigger_workflow_template_async]
    :end-before: [END how_to_cloud_dataproc_trigger_workflow_template_async]

The inline operator is an alternative. It creates a workflow, run it, and delete it afterwards:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateInlineWorkflowTemplateOperator`:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_workflow.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_instantiate_inline_workflow_template]
    :end-before: [END how_to_cloud_dataproc_instantiate_inline_workflow_template]

Also for all this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_workflow_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_instantiate_inline_workflow_template_async]
    :end-before: [END how_to_cloud_dataproc_instantiate_inline_workflow_template_async]


Create a Batch
--------------

Dataproc supports creating a batch workload.

A batch can be created using:
:class: ``~airflow.providers.google.cloud.operators.dataproc.DataprocCreateBatchOperator``.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_batch_operator]
    :end-before: [END how_to_cloud_dataproc_create_batch_operator]

For creating a batch with Persistent History Server first you should create a Dataproc Cluster
with specific parameters. Documentation how create cluster you can find here:
https://cloud.google.com/dataproc/docs/concepts/jobs/history-server#setting_up_a_persistent_history_server

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch_persistent.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_cluster_for_persistent_history_server]
    :end-before: [END how_to_cloud_dataproc_create_cluster_for_persistent_history_server]

After Cluster was created you should add it to the Batch configuration.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch_persistent.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_batch_operator_with_persistent_history_server]
    :end-before: [END how_to_cloud_dataproc_create_batch_operator_with_persistent_history_server]

To check if operation succeeded you can use

:class:`~airflow.providers.google.cloud.sensors.dataproc.DataprocBatchSensor`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_batch_async_sensor]
    :end-before: [END how_to_cloud_dataproc_batch_async_sensor]

Also for all this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch_deferrable.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_batch_operator_async]
    :end-before: [END how_to_cloud_dataproc_create_batch_operator_async]

Get a Batch
-----------

To get a batch you can use:
:class: ``~airflow.providers.google.cloud.operators.dataproc.DataprocGetBatchOperator``.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_get_batch_operator]
    :end-before: [END how_to_cloud_dataproc_get_batch_operator]

List a Batch
------------

To get a list of exists batches you can use:
:class: ``~airflow.providers.google.cloud.operators.dataproc.DataprocListBatchesOperator``.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_list_batches_operator]
    :end-before: [END how_to_cloud_dataproc_list_batches_operator]

Delete a Batch
--------------

To delete a batch you can use:
:class: ``~airflow.providers.google.cloud.operators.dataproc.DataprocDeleteBatchOperator``.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_delete_batch_operator]
    :end-before: [END how_to_cloud_dataproc_delete_batch_operator]

Cancel a Batch Operation
------------------------

To cancel a operation you can use:
:class: ``~airflow.providers.google.cloud.operators.dataproc.DataprocCancelOperationOperator``.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_cancel_operation_operator]
    :end-before: [END how_to_cloud_dataproc_cancel_operation_operator]

References
^^^^^^^^^^
For further information, take a look at:

* `DataProc API documentation <https://cloud.google.com/dataproc/docs/reference>`__
* `Product documentation <https://cloud.google.com/dataproc/docs/reference>`__
