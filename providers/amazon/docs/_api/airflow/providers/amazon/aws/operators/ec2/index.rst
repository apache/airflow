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

:py:mod:`airflow.providers.amazon.aws.operators.ec2`
====================================================

.. py:module:: airflow.providers.amazon.aws.operators.ec2


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.ec2.EC2StartInstanceOperator
   airflow.providers.amazon.aws.operators.ec2.EC2StopInstanceOperator
   airflow.providers.amazon.aws.operators.ec2.EC2CreateInstanceOperator
   airflow.providers.amazon.aws.operators.ec2.EC2TerminateInstanceOperator




.. py:class:: EC2StartInstanceOperator(*, instance_id, aws_conn_id = 'aws_default', region_name = None, check_interval = 15, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Start AWS EC2 instance using boto3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EC2StartInstanceOperator`

   :param instance_id: id of the AWS EC2 instance
   :param aws_conn_id: aws connection to use
   :param region_name: (optional) aws region name associated with the client
   :param check_interval: time in seconds that the job should wait in
       between each instance state checks until operation is completed

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('instance_id', 'region_name')



   .. py:attribute:: ui_color
      :value: '#eeaa11'



   .. py:attribute:: ui_fgcolor
      :value: '#ffffff'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: EC2StopInstanceOperator(*, instance_id, aws_conn_id = 'aws_default', region_name = None, check_interval = 15, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Stop AWS EC2 instance using boto3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EC2StopInstanceOperator`

   :param instance_id: id of the AWS EC2 instance
   :param aws_conn_id: aws connection to use
   :param region_name: (optional) aws region name associated with the client
   :param check_interval: time in seconds that the job should wait in
       between each instance state checks until operation is completed

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('instance_id', 'region_name')



   .. py:attribute:: ui_color
      :value: '#eeaa11'



   .. py:attribute:: ui_fgcolor
      :value: '#ffffff'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: EC2CreateInstanceOperator(image_id, max_count = 1, min_count = 1, aws_conn_id = 'aws_default', region_name = None, poll_interval = 20, max_attempts = 20, config = None, wait_for_completion = False, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Create and start a specified number of EC2 Instances using boto3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EC2CreateInstanceOperator`

   :param image_id: ID of the AMI used to create the instance.
   :param max_count: Maximum number of instances to launch. Defaults to 1.
   :param min_count: Minimum number of instances to launch. Defaults to 1.
   :param aws_conn_id: AWS connection to use
   :param region_name: AWS region name associated with the client.
   :param poll_interval: Number of seconds to wait before attempting to
       check state of instance. Only used if wait_for_completion is True. Default is 20.
   :param max_attempts: Maximum number of attempts when checking state of instance.
       Only used if wait_for_completion is True. Default is 20.
   :param config: Dictionary for arbitrary parameters to the boto3 run_instances call.
   :param wait_for_completion: If True, the operator will wait for the instance to be
       in the `running` state before returning.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('image_id', 'max_count', 'min_count', 'aws_conn_id', 'region_name', 'config', 'wait_for_completion')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: EC2TerminateInstanceOperator(instance_ids, aws_conn_id = 'aws_default', region_name = None, poll_interval = 20, max_attempts = 20, wait_for_completion = False, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Terminate EC2 Instances using boto3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EC2TerminateInstanceOperator`

   :param instance_id: ID of the instance to be terminated.
   :param aws_conn_id: AWS connection to use
   :param region_name: AWS region name associated with the client.
   :param poll_interval: Number of seconds to wait before attempting to
       check state of instance. Only used if wait_for_completion is True. Default is 20.
   :param max_attempts: Maximum number of attempts when checking state of instance.
       Only used if wait_for_completion is True. Default is 20.
   :param wait_for_completion: If True, the operator will wait for the instance to be
       in the `terminated` state before returning.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('instance_ids', 'region_name', 'aws_conn_id', 'wait_for_completion')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
