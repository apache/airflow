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

.. include:: /operators/_partials/prerequisite_tasks.rst


.. _howto/operator:DataprocCreateClusterOperator:
.. _howto/operator:DataprocInstantiateInlineWorkflowTemplateOperator:

Create a Cluster
----------------

When you create a Dataproc cluster, you have the option to choose Compute Engine as the deployment platform.
In this configuration, Dataproc automatically provisions the required Compute Engine VM instances to run the cluster.
The VM instances are used for the main node, primary worker and secondary worker nodes (if specified).
These VM instances are created and managed by Compute Engine, while Dataproc takes care of configuring the software and
orchestration required for the big data processing tasks.
By providing the configuration for your nodes, you describe the configuration of primary and
secondary nodes, and status of a cluster of Compute Engine instances.
Configuring secondary worker nodes, you can specify the number of workers and their types. By
enabling the Preemptible option to use Preemptible VMs (equivalent to Spot instances) for those nodes, you
can take advantage of the cost savings provided by these instances for your Dataproc workloads.
The primary node, which typically hosts the cluster main and various control services, does not have the Preemptible
option because it's crucial for the primary node to maintain stability and availability.
Once a cluster is created, the configuration settings, including the preemptibility of secondary worker nodes,
cannot be modified directly.

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

Dataproc on GKE deploys Dataproc virtual clusters on a GKE cluster. Unlike Dataproc on Compute Engine clusters,
Dataproc on GKE virtual clusters do not include separate main and worker VMs. Instead, when you create a Dataproc on
GKE virtual cluster, Dataproc on GKE creates node pools within a GKE cluster. Dataproc on GKE jobs are run as pods on
these node pools. The node pools and scheduling of pods on the node pools are managed by GKE.

When creating a GKE Dataproc cluster, you can specify the usage of Preemptible VMs for the underlying compute resources.
GKE supports the use of Preemptible VMs as a cost-saving measure.
By enabling Preemptible VMs, GKE will provision the cluster nodes using Preemptible VMs. Or you can create nodes as
Spot VM instances, which are the latest update to legacy preemptible VMs.
This can be beneficial for running Dataproc workloads on GKE while optimizing costs.

To create Dataproc cluster in Google Kubernetes Engine you could pass cluster configuration:

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

You can also create Dataproc cluster with optional component Presto.
To do so, please use the following configuration.
Note that default image might not support the chosen optional component.
If this is your case, please specify correct ``image_version`` that you can find in the
`documentation.  <https://cloud.google.com/dataproc/docs/concepts/components/overview#available_optional_components>`__

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_presto.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_create_cluster]
    :end-before: [END how_to_cloud_dataproc_create_cluster]

You can also create Dataproc cluster with optional component Trino.
To do so, please use the following configuration.
Note that default image might not support the chosen optional component.
If this is your case, please specify correct ``image_version`` that you can find in the
`documentation.  <https://cloud.google.com/dataproc/docs/concepts/components/overview#available_optional_components>`__


.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_trino.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_create_cluster]
    :end-before: [END how_to_cloud_dataproc_create_cluster]

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

Diagnose a cluster
------------------
Dataproc supports the collection of `cluster diagnostic information <https://cloud.google.com/dataproc/docs/support/diagnose-cluster-command#diagnostic_summary_and_archive_contents>`_
like system, Spark, Hadoop, and Dataproc logs, cluster configuration files that can be used to troubleshoot a Dataproc cluster or job.
It is important to note that this information can only be collected before the cluster is deleted.
For more information about the available fields to pass when diagnosing a cluster, visit
`Dataproc diagnose cluster API. <https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/diagnose>`_

To diagnose a Dataproc cluster use:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocDiagnoseClusterOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_diagnose.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_diagnose_cluster]
    :end-before: [END how_to_cloud_dataproc_diagnose_cluster]

You can also use deferrable mode in order to run the operator asynchronously:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_diagnose.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_diagnose_cluster_deferrable]
    :end-before: [END how_to_cloud_dataproc_diagnose_cluster_deferrable]

Update a cluster
----------------
You can scale the cluster up or down by providing a cluster config and a updateMask.
In the updateMask argument you specifies the path, relative to Cluster, of the field to update.
For more information on updateMask and other parameters take a look at `Dataproc update cluster API. <https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters/patch>`__

An example of a new cluster config and the updateMask:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_update.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_updatemask_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_updatemask_cluster_operator]

To update a cluster you can use:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocUpdateClusterOperator`

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_update.py
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

Starting a cluster
---------------------------

To start a cluster you can use the
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocStartClusterOperator`:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_start_stop.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_start_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_start_cluster_operator]

Stopping a cluster
---------------------------

To stop a cluster you can use the
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocStopClusterOperator`:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_cluster_start_stop.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_stop_cluster_operator]
    :end-before: [END how_to_cloud_dataproc_stop_cluster_operator]

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
The list currently includes Spark, PySpark, Hadoop, Trino, Pig, Flink and Hive.
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

Example of the configuration for a SparkR Job:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_sparkr.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_sparkr_config]
    :end-before: [END how_to_cloud_dataproc_sparkr_config]

Example of the configuration for a Presto Job:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_presto.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_presto_config]
    :end-before: [END how_to_cloud_dataproc_presto_config]

Example of the configuration for a Trino Job:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_trino.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_trino_config]
    :end-before: [END how_to_cloud_dataproc_trino_config]

Example of the configuration for a Flink Job:

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_flink.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_flink_config]
    :end-before: [END how_to_cloud_dataproc_flink_config]

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
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocCreateBatchOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_create_batch_operator]
    :end-before: [END how_to_cloud_dataproc_create_batch_operator]

For creating a batch with Persistent History Server first you should create a Dataproc Cluster
with specific parameters. Documentation how create cluster you can find
`here <https://cloud.google.com/dataproc/docs/concepts/jobs/history-server#setting_up_a_persistent_history_server>`__:

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
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocGetBatchOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_get_batch_operator]
    :end-before: [END how_to_cloud_dataproc_get_batch_operator]

List a Batch
------------

To get a list of exists batches you can use:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocListBatchesOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_list_batches_operator]
    :end-before: [END how_to_cloud_dataproc_list_batches_operator]

Delete a Batch
--------------

To delete a batch you can use:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocDeleteBatchOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/dataproc/example_dataproc_batch.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_delete_batch_operator]
    :end-before: [END how_to_cloud_dataproc_delete_batch_operator]

Cancel a Batch Operation
------------------------

To cancel a operation you can use:
:class:`~airflow.providers.google.cloud.operators.dataproc.DataprocCancelOperationOperator`.

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
