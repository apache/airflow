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

:py:mod:`airflow.providers.amazon.aws.operators.cloud_formation`
================================================================

.. py:module:: airflow.providers.amazon.aws.operators.cloud_formation

.. autoapi-nested-parse::

   This module contains CloudFormation create/delete stack operators.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.cloud_formation.CloudFormationCreateStackOperator
   airflow.providers.amazon.aws.operators.cloud_formation.CloudFormationDeleteStackOperator




.. py:class:: CloudFormationCreateStackOperator(*, stack_name, cloudformation_parameters, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An operator that creates a CloudFormation stack.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudFormationCreateStackOperator`

   :param stack_name: stack name (templated)
   :param cloudformation_parameters: parameters to be passed to CloudFormation.
   :param aws_conn_id: aws connection to uses

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('stack_name', 'cloudformation_parameters')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#6b9659'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: CloudFormationDeleteStackOperator(*, stack_name, cloudformation_parameters = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An operator that deletes a CloudFormation stack.

   :param stack_name: stack name (templated)
   :param cloudformation_parameters: parameters to be passed to CloudFormation.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudFormationDeleteStackOperator`

   :param aws_conn_id: aws connection to uses

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('stack_name',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#1d472b'



   .. py:attribute:: ui_fgcolor
      :value: '#FFF'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
