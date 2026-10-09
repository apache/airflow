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



Amazon Executors
================

Each Amazon executor runs every Airflow task in its own container, pod or function
invocation. They differ in which AWS service runs it.

Choosing an executor
--------------------

.. list-table::
    :header-rows: 1
    :widths: 15 25 60

    * - Executor
      - Each task runs as
      - A good fit when
    * - :doc:`ECS <ecs-executor>`
      - An ECS task, on Fargate or EC2
      - You want each task in its own container without running a Kubernetes cluster.
    * - :doc:`Batch <batch-executor>`
      - An AWS Batch job on a job queue
      - You run many tasks at once and want Batch's job queues and priorities, with Batch
        managing Fargate, EC2 or EKS compute for you.
    * - :doc:`EKS <eks-executor>`
      - A pod on an Amazon EKS cluster
      - You already run EKS, or want Kubernetes executor features such as pod templates
        and per-task ``pod_override``.
    * - :doc:`Lambda <lambda-executor>` (experimental)
      - An asynchronous Lambda invocation
      - Tasks are short and light, and always finish within Lambda's 15 minute limit.

You can also run more than one of them side by side, see
:ref:`apache-airflow:using-multiple-executors-concurrently`.


.. toctree::
    :maxdepth: 1

    ECS Executor <ecs-executor>
    Batch Executor <batch-executor>
    EKS Executor <eks-executor>
    Lambda Executor (experimental) <lambda-executor>
