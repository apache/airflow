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

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Overview
--------

Airflow to Amazon EMR on Amazon EKS integration provides a way to run Apache Spark jobs on Kubernetes.

- :class:`~airflow.providers.amazon.aws.operators.emr_containers.EMRContainerOperator`


Create EMR on EKS job with sample script
----------------------------------------

Purpose
"""""""

This example dag ``example_emr_eks_job.py`` uses ``EMRContainerOperator`` to create a new EMR on EKS job calculating the mathematical constant ``Pi``, and monitors the progress
with ``EMRContainerSensor``.

This example assumes that you already have an EMR on EKS virtual cluster configured. See the `EMR on EKS Getting Started guide <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/getting-started.html>`__ for more information.

Environment variables
"""""""""""""""""""""

This example relies on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_emr_eks_job.py
    :language: python
    :start-after: [START howto_operator_emr_eks_env_variables]
    :end-before: [END howto_operator_emr_eks_env_variables]

Job configuration
"""""""""""""""""

To create a job for EMR on EKS, you need to specify your job configuration, any monitoring configuration, and a few other details for the job.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_emr_eks_job.py
    :language: python
    :start-after: [START howto_operator_emr_eks_config]
    :end-before: [END howto_operator_emr_eks_config]

With EMR on EKS, you specify your Spark configuration, the EMR release you want to use, the IAM role to use for the job, and the EMR virtual cluster ID.

We pass the ``virtual_cluster_id`` and ``execution_role_arn`` values as operator parameters, but you can store them in a Connection or provide them in the DAG.

With the EMRContainerOperator, it will wait until the successful completion of the job or raise an ``AirflowException`` if there is an error.

Reference
---------

For further information, look at:

* `Amazon EMR on EKS Job runs <https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/job-runs.html>`__
* `EMR on EKS Best Practices <https://aws.github.io/aws-emr-containers-best-practices/>`__
