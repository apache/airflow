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

Dataproc on Google Distributed Cloud Operators
==============================================

Google Distributed Cloud (GDC) extends Google Cloud's infrastructure and services to customer edge locations and data centers. We intend to bring Dataproc as a managed service offering to GDC. This opens new market opportunities for Dataproc to support customer hybrid cloud strategies, satisfy strict data residency requirements that prevent sending data to GCP and implement a subset of computation at local edge sites, closer to where the data is generated.

Submit a Spark job to a GDC cluster
-----------------------------------

* :class:`~airflow.providers.google.gdc.operators.dataprocgdc.DataprocGDCSubmitSparkJobKrmOperator`
  This class allows you to create and run dataproc spark job on a GDC cluster. This operator simplifies the interface and accepts different parameters to configure and run the DPGDC spark job.

Usage examples
^^^^^^^^^^^^^^

spark_pi_job_template.yaml

.. code-block:: yaml

    ---
      apiVersion: "dataprocgdc.cloud.google.com/v1alpha1"
      kind: SparkApplication
      metadata:
        name: simple-spark
        namespace:
      spec:
        properties:
          "spark.hadoop.fs.s3a.endpoint": "***"
          "spark.hadoop.fs.s3a.access.key": "***"
          "spark.hadoop.fs.s3a.secret.key": "***"
          "spark.hadoop.fs.s3a.path.style.access": "true"
          "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
       sparkApplicationConfig:
          jarFileUris:
            - "file:///usr/lib/spark/examples/jars/spark-examples.jar"
          mainClass: "org.apache.spark.examples.SparkPi"
          args:
            - "10"

create the task using the following:

.. code-block:: python

    operator = DataprocGDCSubmitSparkJobKrmOperator(
        task_id="example-dataprocgdc-submitspark-operator",
        application_file="sparkpi.yaml",
        namespace="default",
        kubernetes_conn_id="myk8s",
    )


Create an App envrionment to a GDC cluster
------------------------------------------

* :class:`~airflow.providers.google.gdc.operators.dataprocgdc.DataprocGdcCreateAppEnvironmentKrmOperator`
  This class allows you to create an app environment on a DPGDC cluster. This operator simplifies the interface and accepts different parameters to configure and create the DPGDC app environment.

Usage examples
^^^^^^^^^^^^^^

app_env_job_template.yaml

.. code-block:: yaml

    ---
      apiVersion: "dataprocgdc.cloud.google.com/v1alpha1"
      kind: ApplicationEnvironment
      metadata:
        name: exampleappenv
        namespace: default
      spec:
        sparkApplicationEnvironmentConfig:
          defaultProperties:
            "spark.hadoop.fs.s3a.endpoint": "***"
            "spark.hadoop.fs.s3a.access.key": "***"
            "spark.hadoop.fs.s3a.secret.key": "***"
            "spark.hadoop.fs.s3a.path.style.access": "true"
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"


create the task using the following:

.. code-block:: python

    operator = DataprocGdcCreateAppEnvironmentKrmOperator(
        task_id="example-dataprocgdc-appenv-operator",
        application_file="appEnv.yaml",
        namespace="default",
        kubernetes_conn_id="myk8s",
    )

Reference
^^^^^^^^^
For further information, look at: https://cloud.google.com/blog/products/infrastructure-modernization/google-distributed-cloud-new-ai-and-data-services
