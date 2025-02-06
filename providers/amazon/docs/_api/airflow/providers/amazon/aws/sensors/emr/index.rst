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

:py:mod:`airflow.providers.amazon.aws.sensors.emr`
==================================================

.. py:module:: airflow.providers.amazon.aws.sensors.emr


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.emr.EmrBaseSensor
   airflow.providers.amazon.aws.sensors.emr.EmrServerlessJobSensor
   airflow.providers.amazon.aws.sensors.emr.EmrServerlessApplicationSensor
   airflow.providers.amazon.aws.sensors.emr.EmrContainerSensor
   airflow.providers.amazon.aws.sensors.emr.EmrNotebookExecutionSensor
   airflow.providers.amazon.aws.sensors.emr.EmrJobFlowSensor
   airflow.providers.amazon.aws.sensors.emr.EmrStepSensor




.. py:class:: EmrBaseSensor(*, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Contains general sensor behavior for EMR.

   Subclasses should implement following methods:
       - ``get_emr_response()``
       - ``state_from_response()``
       - ``failure_message_from_response()``

   Subclasses should set ``target_states`` and ``failed_states`` fields.

   :param aws_conn_id: aws connection to use

   .. py:attribute:: ui_color
      :value: '#66c3ff'



   .. py:method:: get_hook()


   .. py:method:: hook()


   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: get_emr_response(context)
      :abstractmethod:

      Make an API call with boto3 and get response.

      :return: response


   .. py:method:: state_from_response(response)
      :staticmethod:
      :abstractmethod:

      Get state from boto3 response.

      :param response: response from AWS API
      :return: state


   .. py:method:: failure_message_from_response(response)
      :staticmethod:
      :abstractmethod:

      Get state from boto3 response.

      :param response: response from AWS API
      :return: failure message



.. py:class:: EmrServerlessJobSensor(*, application_id, job_run_id, target_states = frozenset(EmrServerlessHook.JOB_SUCCESS_STATES), aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Poll the state of the job run until it reaches a terminal state; fails if the job run fails.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EmrServerlessJobSensor`

   :param application_id: application_id to check the state of
   :param job_run_id: job_run_id to check the state of
   :param target_states: a set of states to wait for, defaults to 'SUCCESS'
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('application_id', 'job_run_id')



   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: hook()

      Create and return an EmrServerlessHook.


   .. py:method:: failure_message_from_response(response)
      :staticmethod:

      Get failure message from response dictionary.

      :param response: response from AWS API
      :return: failure message



.. py:class:: EmrServerlessApplicationSensor(*, application_id, target_states = frozenset(EmrServerlessHook.APPLICATION_SUCCESS_STATES), aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Poll the state of the application until it reaches a terminal state; fails if the application fails.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EmrServerlessApplicationSensor`

   :param application_id: application_id to check the state of
   :param target_states: a set of states to wait for, defaults to {'CREATED', 'STARTED'}
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('application_id',)



   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: hook()

      Create and return an EmrServerlessHook.


   .. py:method:: failure_message_from_response(response)
      :staticmethod:

      Get failure message from response dictionary.

      :param response: response from AWS API
      :return: failure message



.. py:class:: EmrContainerSensor(*, virtual_cluster_id, job_id, max_retries = None, aws_conn_id = 'aws_default', poll_interval = 10, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Poll the state of the job run until it reaches a terminal state; fail if the job run fails.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EmrContainerSensor`

   :param job_id: job_id to check the state of
   :param max_retries: Number of times to poll for query state before
       returning the current state, defaults to None
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'
   :param poll_interval: Time in seconds to wait between two consecutive call to
       check query status on athena, defaults to 10
   :param deferrable: Run sensor in the deferrable mode.

   .. py:attribute:: INTERMEDIATE_STATES
      :value: ('PENDING', 'SUBMITTED', 'RUNNING')



   .. py:attribute:: FAILURE_STATES
      :value: ('FAILED', 'CANCELLED', 'CANCEL_PENDING')



   .. py:attribute:: SUCCESS_STATES
      :value: ('COMPLETED',)



   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('virtual_cluster_id', 'job_id')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#66c3ff'



   .. py:method:: hook()


   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: EmrNotebookExecutionSensor(notebook_execution_id, target_states = None, failed_states = None, **kwargs)


   Bases: :py:obj:`EmrBaseSensor`

   Poll the EMR notebook until it reaches any of the target states; raise AirflowException on failure.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EmrNotebookExecutionSensor`

   :param notebook_execution_id: Unique id of the notebook execution to be poked.
   :target_states: the states the sensor will wait for the execution to reach.
       Default target_states is ``FINISHED``.
   :failed_states: if the execution reaches any of the failed_states, the sensor will fail.
       Default failed_states is ``FAILED``.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('notebook_execution_id',)



   .. py:attribute:: FAILURE_STATES



   .. py:attribute:: COMPLETED_STATES



   .. py:method:: get_emr_response(context)

      Make an API call with boto3 and get response.

      :return: response


   .. py:method:: state_from_response(response)
      :staticmethod:

      Make an API call with boto3 and get cluster-level details.

      .. seealso::
          https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_cluster

      :return: response


   .. py:method:: failure_message_from_response(response)
      :staticmethod:

      Get failure message from response dictionary.

      :param response: response from AWS API
      :return: failure message



.. py:class:: EmrJobFlowSensor(*, job_flow_id, target_states = None, failed_states = None, max_attempts = 60, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`EmrBaseSensor`

   Poll the EMR JobFlow Cluster until it reaches any of the target states; raise AirflowException on failure.

   With the default target states, sensor waits cluster to be terminated.
   When target_states is set to ['RUNNING', 'WAITING'] sensor waits
   until job flow to be ready (after 'STARTING' and 'BOOTSTRAPPING' states)

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EmrJobFlowSensor`

   :param job_flow_id: job_flow_id to check the state of
   :param target_states: the target states, sensor waits until
       job flow reaches any of these states. In deferrable mode it would
       run until reach the terminal state.
   :param failed_states: the failure states, sensor fails when
       job flow reaches any of these states
   :param max_attempts: Maximum number of tries before failing
   :param deferrable: Run sensor in the deferrable mode.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_flow_id', 'target_states', 'failed_states')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: get_emr_response(context)

      Make an API call with boto3 and get cluster-level details.

      .. seealso::
          https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_cluster

      :return: response


   .. py:method:: state_from_response(response)
      :staticmethod:

      Get state from response dictionary.

      :param response: response from AWS API
      :return: current state of the cluster


   .. py:method:: failure_message_from_response(response)
      :staticmethod:

      Get failure message from response dictionary.

      :param response: response from AWS API
      :return: failure message


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: EmrStepSensor(*, job_flow_id, step_id, target_states = None, failed_states = None, max_attempts = 60, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`EmrBaseSensor`

   Poll the state of the step until it reaches any of the target states; raise AirflowException on failure.

   With the default target states, sensor waits step to be completed.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EmrStepSensor`

   :param job_flow_id: job_flow_id which contains the step check the state of
   :param step_id: step to check the state of
   :param target_states: the target states, sensor waits until
       step reaches any of these states. In case of deferrable sensor it will
       for reach to terminal state
   :param failed_states: the failure states, sensor fails when
       step reaches any of these states
   :param max_attempts: Maximum number of tries before failing
   :param deferrable: Run sensor in the deferrable mode.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_flow_id', 'step_id', 'target_states', 'failed_states')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: get_emr_response(context)

      Make an API call with boto3 and get details about the cluster step.

      .. seealso::
          https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

      :return: response


   .. py:method:: state_from_response(response)
      :staticmethod:

      Get state from response dictionary.

      :param response: response from AWS API
      :return: execution state of the cluster step


   .. py:method:: failure_message_from_response(response)
      :staticmethod:

      Get failure message from response dictionary.

      :param response: response from AWS API
      :return: failure message


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)
