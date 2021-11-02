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


.. _howto/operator:EMRContainersOperators:

Amazon EMR on EKS Operators
===========================

`Amazon EMR on EKS <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks.html>`__  provides a deployment option for Amazon EMR that allows you to run open-source big data frameworks on Amazon Elastic Kubernetes Service (Amazon EKS).

Airflow provides the :class:`~airflow.providers.amazon.aws.operators.emr_containers.EMRContainerOperator` to submit Spark jobs to your EMR on EKS virtual cluster.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

This example assumes that you already have an EMR on EKS virtual cluster configured. See the `EMR on EKS Getting Started guide <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/getting-started.html>`__ for more information.


Run a Spark job on EMR on EKS
-----------------------------

Purpose
"""""""

The ``EMRContainerOperator`` will submit a new job to an EMR on EKS virtual cluster and wait for the job to complete. The example job below calculates the mathematical constant ``Pi``, and monitors the progress with ``EMRContainerSensor``. In a production job, you would usually refer to a Spark script on Amazon S3.

Job configuration
"""""""""""""""""

To create a job for EMR on EKS, you need to specify your virtual cluster ID, the release of EMR you want to use, your IAM execution role, and Spark submit parameters.

You can also optionally provide configuration overrides such as Spark, Hive, or Log4j properties as well as monitoring configuration that sends Spark logs to S3 or Cloudwatch.

In the example, we show how to add an ``applicationConfiguration`` to use the AWS Glue data catalog and ``monitoringConfiguration`` to send logs to the ``/aws/emr-eks-spark`` log group in CloudWatch. Refer to the `EMR on EKS guide <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks-jobs-CLI.html#emr-eks-jobs-parameters>`__ for more details on job configuration.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_emr_eks_job.py
    :language: python
    :start-after: [START howto_operator_emr_eks_config]
    :end-before: [END howto_operator_emr_eks_config]


We pass the ``virtual_cluster_id`` and ``execution_role_arn`` values as operator parameters, but you can store them in a connection or provide them in the DAG. Your AWS region should be defined either in the ``aws_default`` connection as ``{"region_name": "us-east-1"}`` or a custom connection name that gets passed to the operator with the ``aws_conn_id`` parameter.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_emr_eks_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_emr_eks_jobrun]
    :end-before: [END howto_operator_emr_eks_jobrun]

With the EMRContainerOperator, it will wait until the successful completion of the job or raise an ``AirflowException`` if there is an error. The operator returns the Job ID of the job run.

Reference
---------

For further information, look at:

* `Amazon EMR on EKS Job runs <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/job-runs.html>`__
* `EMR on EKS Best Practices <https://aws.github.io/aws-emr-containers-best-practices/>`__
