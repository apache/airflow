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


Supported Deferrable Operators
==========================================

List of operators that supports deferrable mode

.. START automatically generated

**apache-airflow**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.operators.trigger_dagrun`
     - :mod:`TriggerDagRunOperator`


**apache-airflow-providers-amazon**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.providers.amazon.aws.sensors.emr`
     - :mod:`EmrContainerSensor`

   * - :mod:`airflow.providers.amazon.aws.sensors.emr`
     - :mod:`EmrJobFlowSensor`

   * - :mod:`airflow.providers.amazon.aws.sensors.emr`
     - :mod:`EmrStepSensor`

   * - :mod:`airflow.providers.amazon.aws.sensors.batch`
     - :mod:`BatchSensor`

   * - :mod:`airflow.providers.amazon.aws.sensors.ec2`
     - :mod:`EC2InstanceStateSensor`

   * - :mod:`airflow.providers.amazon.aws.sensors.s3`
     - :mod:`S3KeySensor`

   * - :mod:`airflow.providers.amazon.aws.sensors.s3`
     - :mod:`S3KeysUnchangedSensor`

   * - :mod:`airflow.providers.amazon.aws.operators.rds`
     - :mod:`RdsCreateDbInstanceOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.rds`
     - :mod:`RdsDeleteDbInstanceOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.emr`
     - :mod:`EmrAddStepsOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.emr`
     - :mod:`EmrContainerOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.emr`
     - :mod:`EmrCreateJobFlowOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.emr`
     - :mod:`EmrTerminateJobFlowOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.redshift_cluster`
     - :mod:`RedshiftCreateClusterOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.redshift_cluster`
     - :mod:`RedshiftCreateClusterSnapshotOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.redshift_cluster`
     - :mod:`RedshiftResumeClusterOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.redshift_cluster`
     - :mod:`RedshiftPauseClusterOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.redshift_cluster`
     - :mod:`RedshiftDeleteClusterOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.eks`
     - :mod:`EksCreateNodegroupOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.eks`
     - :mod:`EksCreateFargateProfileOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.eks`
     - :mod:`EksDeleteNodegroupOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.eks`
     - :mod:`EksDeleteFargateProfileOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.batch`
     - :mod:`BatchOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.batch`
     - :mod:`BatchCreateComputeEnvironmentOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.glue`
     - :mod:`GlueJobOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.sagemaker`
     - :mod:`SageMakerProcessingOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.sagemaker`
     - :mod:`SageMakerEndpointOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.sagemaker`
     - :mod:`SageMakerTransformOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.sagemaker`
     - :mod:`SageMakerTuningOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.sagemaker`
     - :mod:`SageMakerTrainingOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.glue_crawler`
     - :mod:`GlueCrawlerOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.athena`
     - :mod:`AthenaOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.ecs`
     - :mod:`EcsCreateClusterOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.ecs`
     - :mod:`EcsDeleteClusterOperator`

   * - :mod:`airflow.providers.amazon.aws.operators.ecs`
     - :mod:`EcsRunTaskOperator`


**apache-airflow-providers-dbt-cloud**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.providers.dbt.cloud.sensors.dbt`
     - :mod:`DbtCloudJobRunSensor`

   * - :mod:`airflow.providers.dbt.cloud.operators.dbt`
     - :mod:`DbtCloudRunJobOperator`


**apache-airflow-providers-google**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.providers.google.cloud.sensors.gcs`
     - :mod:`GCSObjectExistenceSensor`

   * - :mod:`airflow.providers.google.cloud.sensors.gcs`
     - :mod:`GCSObjectUpdateSensor`

   * - :mod:`airflow.providers.google.cloud.sensors.gcs`
     - :mod:`GCSObjectsWithPrefixExistenceSensor`

   * - :mod:`airflow.providers.google.cloud.sensors.gcs`
     - :mod:`GCSUploadSessionCompleteSensor`

   * - :mod:`airflow.providers.google.cloud.sensors.bigquery`
     - :mod:`BigQueryTableExistenceSensor`

   * - :mod:`airflow.providers.google.cloud.sensors.bigquery`
     - :mod:`BigQueryTablePartitionExistenceSensor`

   * - :mod:`airflow.providers.google.cloud.sensors.pubsub`
     - :mod:`PubSubPullSensor`

   * - :mod:`airflow.providers.google.cloud.operators.kubernetes_engine`
     - :mod:`GKEDeleteClusterOperator`

   * - :mod:`airflow.providers.google.cloud.operators.kubernetes_engine`
     - :mod:`GKECreateClusterOperator`

   * - :mod:`airflow.providers.google.cloud.operators.bigquery`
     - :mod:`BigQueryCheckOperator`

   * - :mod:`airflow.providers.google.cloud.operators.bigquery`
     - :mod:`BigQueryValueCheckOperator`

   * - :mod:`airflow.providers.google.cloud.operators.bigquery`
     - :mod:`BigQueryIntervalCheckOperator`

   * - :mod:`airflow.providers.google.cloud.operators.bigquery`
     - :mod:`BigQueryGetDataOperator`

   * - :mod:`airflow.providers.google.cloud.operators.bigquery`
     - :mod:`BigQueryInsertJobOperator`

   * - :mod:`airflow.providers.google.cloud.operators.cloud_sql`
     - :mod:`CloudSQLExportInstanceOperator`

   * - :mod:`airflow.providers.google.cloud.operators.cloud_composer`
     - :mod:`CloudComposerCreateEnvironmentOperator`

   * - :mod:`airflow.providers.google.cloud.operators.cloud_composer`
     - :mod:`CloudComposerDeleteEnvironmentOperator`

   * - :mod:`airflow.providers.google.cloud.operators.cloud_composer`
     - :mod:`CloudComposerUpdateEnvironmentOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataproc`
     - :mod:`DataprocCreateClusterOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataproc`
     - :mod:`DataprocDeleteClusterOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataproc`
     - :mod:`DataprocJobBaseOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataproc`
     - :mod:`DataprocInstantiateWorkflowTemplateOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataproc`
     - :mod:`DataprocInstantiateInlineWorkflowTemplateOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataproc`
     - :mod:`DataprocSubmitJobOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataproc`
     - :mod:`DataprocUpdateClusterOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataproc`
     - :mod:`DataprocCreateBatchOperator`

   * - :mod:`airflow.providers.google.cloud.operators.mlengine`
     - :mod:`MLEngineStartTrainingJobOperator`

   * - :mod:`airflow.providers.google.cloud.operators.bigquery_dts`
     - :mod:`BigQueryDataTransferServiceStartTransferRunsOperator`

   * - :mod:`airflow.providers.google.cloud.operators.datafusion`
     - :mod:`CloudDataFusionStartPipelineOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataflow`
     - :mod:`DataflowTemplatedJobStartOperator`

   * - :mod:`airflow.providers.google.cloud.operators.dataflow`
     - :mod:`DataflowStartFlexTemplateOperator`

   * - :mod:`airflow.providers.google.cloud.operators.cloud_build`
     - :mod:`CloudBuildCreateBuildOperator`


**apache-airflow-providers-cncf-kubernetes**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.providers.cncf.kubernetes.operators.pod`
     - :mod:`KubernetesPodOperator`


**apache-airflow-providers-microsoft-azure**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.providers.microsoft.azure.sensors.data_factory`
     - :mod:`AzureDataFactoryPipelineRunStatusSensor`

   * - :mod:`airflow.providers.microsoft.azure.sensors.wasb`
     - :mod:`WasbBlobSensor`

   * - :mod:`airflow.providers.microsoft.azure.sensors.wasb`
     - :mod:`WasbPrefixSensor`

   * - :mod:`airflow.providers.microsoft.azure.operators.data_factory`
     - :mod:`AzureDataFactoryRunPipelineOperator`


**apache-airflow-providers-databricks**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.providers.databricks.operators.databricks`
     - :mod:`DatabricksSubmitRunOperator`

   * - :mod:`airflow.providers.databricks.operators.databricks`
     - :mod:`DatabricksRunNowOperator`


**apache-airflow-providers-snowflake**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.providers.snowflake.operators.snowflake`
     - :mod:`SnowflakeSqlApiOperator`


**apache-airflow-providers-http**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.providers.http.operators.http`
     - :mod:`SimpleHttpOperator`


**apache-airflow-providers-apache-livy**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators

   * - :mod:`airflow.providers.apache.livy.operators.livy`
     - :mod:`LivyOperator`

.. END automatically generated
