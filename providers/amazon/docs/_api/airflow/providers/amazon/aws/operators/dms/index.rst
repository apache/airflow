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

:py:mod:`airflow.providers.amazon.aws.operators.dms`
====================================================

.. py:module:: airflow.providers.amazon.aws.operators.dms


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.dms.DmsCreateTaskOperator
   airflow.providers.amazon.aws.operators.dms.DmsDeleteTaskOperator
   airflow.providers.amazon.aws.operators.dms.DmsDescribeTasksOperator
   airflow.providers.amazon.aws.operators.dms.DmsStartTaskOperator
   airflow.providers.amazon.aws.operators.dms.DmsStopTaskOperator




.. py:class:: DmsCreateTaskOperator(*, replication_task_id, source_endpoint_arn, target_endpoint_arn, replication_instance_arn, table_mappings, migration_type = 'full-load', create_task_kwargs = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates AWS DMS replication task.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DmsCreateTaskOperator`

   :param replication_task_id: Replication task id
   :param source_endpoint_arn: Source endpoint ARN
   :param target_endpoint_arn: Target endpoint ARN
   :param replication_instance_arn: Replication instance ARN
   :param table_mappings: Table mappings
   :param migration_type: Migration type ('full-load'|'cdc'|'full-load-and-cdc'), full-load by default.
   :param create_task_kwargs: Extra arguments for DMS replication task creation.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('replication_task_id', 'source_endpoint_arn', 'target_endpoint_arn',...



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: template_fields_renderers



   .. py:method:: execute(context)

      Create AWS DMS replication task from Airflow.

      :return: replication task arn



.. py:class:: DmsDeleteTaskOperator(*, replication_task_arn = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Deletes AWS DMS replication task.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DmsDeleteTaskOperator`

   :param replication_task_arn: Replication task ARN
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('replication_task_arn',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: template_fields_renderers
      :type: dict[str, str]



   .. py:method:: execute(context)

      Delete AWS DMS replication task from Airflow.

      :return: replication task arn



.. py:class:: DmsDescribeTasksOperator(*, describe_tasks_kwargs = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Describes AWS DMS replication tasks.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DmsDescribeTasksOperator`

   :param describe_tasks_kwargs: Describe tasks command arguments
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('describe_tasks_kwargs',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: template_fields_renderers
      :type: dict[str, str]



   .. py:method:: execute(context)

      Describe AWS DMS replication tasks from Airflow.

      :return: Marker and list of replication tasks



.. py:class:: DmsStartTaskOperator(*, replication_task_arn, start_replication_task_type = 'start-replication', start_task_kwargs = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Starts AWS DMS replication task.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DmsStartTaskOperator`

   :param replication_task_arn: Replication task ARN
   :param start_replication_task_type: Replication task start type (default='start-replication')
       ('start-replication'|'resume-processing'|'reload-target')
   :param start_task_kwargs: Extra start replication task arguments
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('replication_task_arn', 'start_replication_task_type', 'start_task_kwargs')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: template_fields_renderers



   .. py:method:: execute(context)

      Start AWS DMS replication task from Airflow.

      :return: replication task arn



.. py:class:: DmsStopTaskOperator(*, replication_task_arn = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Stops AWS DMS replication task.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DmsStopTaskOperator`

   :param replication_task_arn: Replication task ARN
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('replication_task_arn',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: template_fields_renderers
      :type: dict[str, str]



   .. py:method:: execute(context)

      Stop AWS DMS replication task from Airflow.

      :return: replication task arn
