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

:py:mod:`airflow.providers.amazon.aws.executors.ecs.boto_schema`
================================================================

.. py:module:: airflow.providers.amazon.aws.executors.ecs.boto_schema

.. autoapi-nested-parse::

   AWS ECS Executor Boto Schema.

   Schemas for easily and consistently parsing boto responses.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.executors.ecs.boto_schema.BotoContainerSchema
   airflow.providers.amazon.aws.executors.ecs.boto_schema.BotoTaskSchema
   airflow.providers.amazon.aws.executors.ecs.boto_schema.BotoFailureSchema
   airflow.providers.amazon.aws.executors.ecs.boto_schema.BotoRunTaskSchema
   airflow.providers.amazon.aws.executors.ecs.boto_schema.BotoDescribeTasksSchema




.. py:class:: BotoContainerSchema(*, only = None, exclude = (), many = False, context = None, load_only = (), dump_only = (), partial = None, unknown = None)


   Bases: :py:obj:`marshmallow.Schema`

   Botocore Serialization Object for ECS ``Container`` shape.

   Note that there are many more parameters, but the executor only needs the members listed below.

   .. py:class:: Meta


      Options object for a Schema. See Schema.Meta for more details and valid values.

      .. py:attribute:: unknown




   .. py:attribute:: exit_code



   .. py:attribute:: last_status



   .. py:attribute:: name




.. py:class:: BotoTaskSchema(*, only = None, exclude = (), many = False, context = None, load_only = (), dump_only = (), partial = None, unknown = None)


   Bases: :py:obj:`marshmallow.Schema`

   Botocore Serialization Object for ECS ``Task`` shape.

   Note that there are many more parameters, but the executor only needs the members listed below.

   .. py:class:: Meta


      Options object for a Schema. See Schema.Meta for more details and valid values.

      .. py:attribute:: unknown




   .. py:attribute:: task_arn



   .. py:attribute:: last_status



   .. py:attribute:: desired_status



   .. py:attribute:: containers



   .. py:attribute:: started_at



   .. py:attribute:: stopped_reason



   .. py:method:: make_task(data, **kwargs)

      Overwrites marshmallow load() to return an instance of EcsExecutorTask instead of a dictionary.



.. py:class:: BotoFailureSchema(*, only = None, exclude = (), many = False, context = None, load_only = (), dump_only = (), partial = None, unknown = None)


   Bases: :py:obj:`marshmallow.Schema`

   Botocore Serialization Object for ECS ``Failure`` Shape.

   .. py:class:: Meta


      Options object for a Schema. See Schema.Meta for more details and valid values.

      .. py:attribute:: unknown




   .. py:attribute:: arn



   .. py:attribute:: reason




.. py:class:: BotoRunTaskSchema(*, only = None, exclude = (), many = False, context = None, load_only = (), dump_only = (), partial = None, unknown = None)


   Bases: :py:obj:`marshmallow.Schema`

   Botocore Serialization Object for ECS ``RunTask`` Operation output.

   .. py:class:: Meta


      Options object for a Schema. See Schema.Meta for more details and valid values.

      .. py:attribute:: unknown




   .. py:attribute:: tasks



   .. py:attribute:: failures




.. py:class:: BotoDescribeTasksSchema(*, only = None, exclude = (), many = False, context = None, load_only = (), dump_only = (), partial = None, unknown = None)


   Bases: :py:obj:`marshmallow.Schema`

   Botocore Serialization Object for ECS ``DescribeTask`` Operation output.

   .. py:class:: Meta


      Options object for a Schema. See Schema.Meta for more details and valid values.

      .. py:attribute:: unknown




   .. py:attribute:: tasks



   .. py:attribute:: failures
