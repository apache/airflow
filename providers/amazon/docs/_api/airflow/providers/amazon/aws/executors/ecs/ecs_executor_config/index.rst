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

:py:mod:`airflow.providers.amazon.aws.executors.ecs.ecs_executor_config`
========================================================================

.. py:module:: airflow.providers.amazon.aws.executors.ecs.ecs_executor_config

.. autoapi-nested-parse::

   AWS ECS Executor configuration.

   This is the configuration for calling the ECS ``run_task`` function. The AWS ECS Executor calls
   Boto3's ``run_task(**kwargs)`` function with the kwargs templated by this dictionary. See the URL
   below for documentation on the parameters accepted by the Boto3 run_task function.

   .. seealso::
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.executors.ecs.ecs_executor_config.build_task_kwargs



.. py:function:: build_task_kwargs()
