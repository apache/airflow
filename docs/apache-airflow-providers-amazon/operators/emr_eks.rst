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

========================
Amazon EMR on Amazon EKS
========================

`Amazon EMR on EKS <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks.html>`__
provides a deployment option for Amazon EMR that allows you to run open-source big data frameworks on
Amazon EKS.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------


.. _howto/operator:EmrEksCreateClusterOperator:


Create an Amazon EMR EKS virtual cluster
========================================


The ``EmrEksCreateClusterOperator`` will create an Amazon EMR on EKS virtual cluster.
The example DAG below shows how to create an EMR on EKS virtual cluster.

To create an Amazon EMR cluster on Amazon EKS, you need to specify a virtual cluster name,
the eks cluster that you would like to use , and an eks namespace.

Refer to the `EMR on EKS Development guide <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/virtual-cluster.html>`__
for more details.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_emr_eks.py
    :language: python
    :start-after: [START howto_operator_emr_eks_create_cluster]
    :end-before: [END howto_operator_emr_eks_create_cluster]



.. _howto/operator:EmrContainerOperator:


Submit a job to an Amazon EMR virtual cluster
=============================================

.. note::
  This example assumes that you already have an EMR on EKS virtual cluster configured. See the
  `EMR on EKS Getting Started guide <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/getting-started.html>`__
  for more information.

The ``EmrContainerOperator`` will submit a new job to an Amazon EMR on Amazon EKS virtual cluster
The example job below calculates the mathematical constant ``Pi``. In a
production job, you would usually refer to a Spark script on Amazon Simple Storage Service (S3).

To create a job for Amazon EMR on Amazon EKS, you need to specify your virtual cluster ID, the release of Amazon EMR you
want to use, your IAM execution role, and Spark submit parameters.

You can also optionally provide configuration overrides such as Spark, Hive, or Log4j properties as
well as monitoring configuration that sends Spark logs to Amazon S3 or Amazon Cloudwatch.

In the example, we show how to add an ``applicationConfiguration`` to use the AWS Glue data catalog
and ``monitoringConfiguration`` to send logs to the ``/aws/emr-eks-spark`` log group in Amazon CloudWatch.
Refer to the `EMR on EKS guide <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/emr-eks-jobs-CLI.html#emr-eks-jobs-parameters>`__
for more details on job configuration.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_emr_eks.py
    :language: python
    :start-after: [START howto_operator_emr_eks_config]
    :end-before: [END howto_operator_emr_eks_config]

We pass the ``virtual_cluster_id`` and ``execution_role_arn`` values as operator parameters, but you
can store them in a connection or provide them in the DAG. Your AWS region should be defined either
in the ``aws_default`` connection as ``{"region_name": "us-east-1"}`` or a custom connection name
that gets passed to the operator with the ``aws_conn_id`` parameter. The operator returns the Job ID of the job run.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_emr_eks.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_emr_container]
    :end-before: [END howto_operator_emr_container]


Sensors
-------

.. _howto/sensor:EmrContainerSensor:

Wait on an Amazon EMR virtual cluster job
=========================================

To wait on the status of an Amazon EMR virtual cluster job to reach a terminal state, you can use
:class:`~airflow.providers.amazon.aws.sensors.emr.EmrContainerSensor`

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_emr_eks.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_emr_container]
    :end-before: [END howto_sensor_emr_container]

Reference
---------

* `AWS boto3 library documentation for EMR Containers <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr-containers.html>`__
* `Amazon EMR on EKS Job runs <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/job-runs.html>`__
* `EMR on EKS Best Practices <https://aws.github.io/aws-emr-containers-best-practices/>`__
