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

:py:mod:`airflow.providers.amazon.aws.sensors.eks`
==================================================

.. py:module:: airflow.providers.amazon.aws.sensors.eks

.. autoapi-nested-parse::

   Tracking the state of Amazon EKS Clusters, Amazon EKS managed node groups, and AWS Fargate profiles.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.eks.EksBaseSensor
   airflow.providers.amazon.aws.sensors.eks.EksClusterStateSensor
   airflow.providers.amazon.aws.sensors.eks.EksFargateProfileStateSensor
   airflow.providers.amazon.aws.sensors.eks.EksNodegroupStateSensor




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.eks.DEFAULT_CONN_ID
   airflow.providers.amazon.aws.sensors.eks.CLUSTER_TERMINAL_STATES
   airflow.providers.amazon.aws.sensors.eks.FARGATE_TERMINAL_STATES
   airflow.providers.amazon.aws.sensors.eks.NODEGROUP_TERMINAL_STATES


.. py:data:: DEFAULT_CONN_ID
   :value: 'aws_default'



.. py:data:: CLUSTER_TERMINAL_STATES



.. py:data:: FARGATE_TERMINAL_STATES



.. py:data:: NODEGROUP_TERMINAL_STATES



.. py:class:: EksBaseSensor(*, cluster_name, target_state, target_state_type, aws_conn_id = DEFAULT_CONN_ID, region = None, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Base class to check various EKS states.

   Subclasses need to implement get_state and get_terminal_states methods.

   :param cluster_name: The name of the Cluster
   :param target_state: Will return successfully when that state is reached.
   :param target_state_type: The enum containing the states,
       will be used to convert the target state if it has to be converted from a string
   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then the default boto3 configuration would be used (and must be
       maintained on each worker node).
   :param region: Which AWS region the connection should use.
       If this is None or empty then the default boto3 behaviour is used.

   .. py:method:: hook()


   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: get_state()
      :abstractmethod:


   .. py:method:: get_terminal_states()
      :abstractmethod:



.. py:class:: EksClusterStateSensor(*, target_state = ClusterStates.ACTIVE, **kwargs)


   Bases: :py:obj:`EksBaseSensor`

   Check the state of an Amazon EKS Cluster until it reaches the target state or another terminal state.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EksClusterStateSensor`

   :param cluster_name: The name of the Cluster to watch. (templated)
   :param target_state: Target state of the Cluster. (templated)
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'target_state', 'aws_conn_id', 'region')



   .. py:attribute:: ui_color
      :value: '#ff9900'



   .. py:attribute:: ui_fgcolor
      :value: '#232F3E'



   .. py:method:: get_state()


   .. py:method:: get_terminal_states()



.. py:class:: EksFargateProfileStateSensor(*, fargate_profile_name, target_state = FargateProfileStates.ACTIVE, **kwargs)


   Bases: :py:obj:`EksBaseSensor`

   Check the state of an AWS Fargate profile until it reaches the target state or another terminal state.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EksFargateProfileStateSensor`

   :param cluster_name: The name of the Cluster which the AWS Fargate profile is attached to. (templated)
   :param fargate_profile_name: The name of the Fargate profile to watch. (templated)
   :param target_state: Target state of the Fargate profile. (templated)
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'fargate_profile_name', 'target_state', 'aws_conn_id', 'region')



   .. py:attribute:: ui_color
      :value: '#ff9900'



   .. py:attribute:: ui_fgcolor
      :value: '#232F3E'



   .. py:method:: get_state()


   .. py:method:: get_terminal_states()



.. py:class:: EksNodegroupStateSensor(*, nodegroup_name, target_state = NodegroupStates.ACTIVE, **kwargs)


   Bases: :py:obj:`EksBaseSensor`

   Check the state of an EKS managed node group until it reaches the target state or another terminal state.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EksNodegroupStateSensor`

   :param cluster_name: The name of the Cluster which the Nodegroup is attached to. (templated)
   :param nodegroup_name: The name of the Nodegroup to watch. (templated)
   :param target_state: Target state of the Nodegroup. (templated)
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'nodegroup_name', 'target_state', 'aws_conn_id', 'region')



   .. py:attribute:: ui_color
      :value: '#ff9900'



   .. py:attribute:: ui_fgcolor
      :value: '#232F3E'



   .. py:method:: get_state()


   .. py:method:: get_terminal_states()
