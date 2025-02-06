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

:py:mod:`airflow.providers.amazon.aws.sensors.sagemaker`
========================================================

.. py:module:: airflow.providers.amazon.aws.sensors.sagemaker


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.sagemaker.SageMakerBaseSensor
   airflow.providers.amazon.aws.sensors.sagemaker.SageMakerEndpointSensor
   airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTransformSensor
   airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTuningSensor
   airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTrainingSensor
   airflow.providers.amazon.aws.sensors.sagemaker.SageMakerPipelineSensor
   airflow.providers.amazon.aws.sensors.sagemaker.SageMakerAutoMLSensor




.. py:class:: SageMakerBaseSensor(*, aws_conn_id = 'aws_default', resource_type = 'job', **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Contains general sensor behavior for SageMaker.

   Subclasses should implement get_sagemaker_response() and state_from_response() methods.
   Subclasses should also implement NON_TERMINAL_STATES and FAILED_STATE methods.

   .. py:attribute:: ui_color
      :value: '#ededed'



   .. py:method:: get_hook()

      Get SageMakerHook.


   .. py:method:: hook()


   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: non_terminal_states()
      :abstractmethod:

      Return states with should not terminate.


   .. py:method:: failed_states()
      :abstractmethod:

      Return states with are considered failed.


   .. py:method:: get_sagemaker_response()
      :abstractmethod:

      Check status of a SageMaker task.


   .. py:method:: get_failed_reason_from_response(response)

      Extract the reason for failure from an AWS response.


   .. py:method:: state_from_response(response)
      :abstractmethod:

      Extract the state from an AWS response.



.. py:class:: SageMakerEndpointSensor(*, endpoint_name, **kwargs)


   Bases: :py:obj:`SageMakerBaseSensor`

   Poll the endpoint state until it reaches a terminal state; raise AirflowException with the failure reason.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:SageMakerEndpointSensor`

   :param endpoint_name: Name of the endpoint instance to watch.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('endpoint_name',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:method:: non_terminal_states()

      Return states with should not terminate.


   .. py:method:: failed_states()

      Return states with are considered failed.


   .. py:method:: get_sagemaker_response()

      Check status of a SageMaker task.


   .. py:method:: get_failed_reason_from_response(response)

      Extract the reason for failure from an AWS response.


   .. py:method:: state_from_response(response)

      Extract the state from an AWS response.



.. py:class:: SageMakerTransformSensor(*, job_name, **kwargs)


   Bases: :py:obj:`SageMakerBaseSensor`

   Poll the transform job until it reaches a terminal state; raise AirflowException with the failure reason.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:SageMakerTransformSensor`

   :param job_name: Name of the transform job to watch.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_name',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:method:: non_terminal_states()

      Return states with should not terminate.


   .. py:method:: failed_states()

      Return states with are considered failed.


   .. py:method:: get_sagemaker_response()

      Check status of a SageMaker task.


   .. py:method:: get_failed_reason_from_response(response)

      Extract the reason for failure from an AWS response.


   .. py:method:: state_from_response(response)

      Extract the state from an AWS response.



.. py:class:: SageMakerTuningSensor(*, job_name, **kwargs)


   Bases: :py:obj:`SageMakerBaseSensor`

   Poll the tuning state until it reaches a terminal state; raise AirflowException with the failure reason.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:SageMakerTuningSensor`

   :param job_name: Name of the tuning instance to watch.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_name',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:method:: non_terminal_states()

      Return states with should not terminate.


   .. py:method:: failed_states()

      Return states with are considered failed.


   .. py:method:: get_sagemaker_response()

      Check status of a SageMaker task.


   .. py:method:: get_failed_reason_from_response(response)

      Extract the reason for failure from an AWS response.


   .. py:method:: state_from_response(response)

      Extract the state from an AWS response.



.. py:class:: SageMakerTrainingSensor(*, job_name, print_log=True, **kwargs)


   Bases: :py:obj:`SageMakerBaseSensor`

   Poll the training job until it reaches a terminal state; raise AirflowException with the failure reason.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:SageMakerTrainingSensor`

   :param job_name: Name of the training job to watch.
   :param print_log: Prints the cloudwatch log if True; Defaults to True.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_name',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:method:: init_log_resource(hook)

      Set tailing LogState for associated training job.


   .. py:method:: non_terminal_states()

      Return states with should not terminate.


   .. py:method:: failed_states()

      Return states with are considered failed.


   .. py:method:: get_sagemaker_response()

      Check status of a SageMaker task.


   .. py:method:: get_failed_reason_from_response(response)

      Extract the reason for failure from an AWS response.


   .. py:method:: state_from_response(response)

      Extract the state from an AWS response.



.. py:class:: SageMakerPipelineSensor(*, pipeline_exec_arn, verbose = True, **kwargs)


   Bases: :py:obj:`SageMakerBaseSensor`

   Poll the pipeline until it reaches a terminal state; raise AirflowException with the failure reason.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:SageMakerPipelineSensor`

   :param pipeline_exec_arn: ARN of the pipeline to watch.
   :param verbose: Whether to print steps details while waiting for completion.
           Defaults to true, consider turning off for pipelines that have thousands of steps.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('pipeline_exec_arn',)



   .. py:method:: non_terminal_states()

      Return states with should not terminate.


   .. py:method:: failed_states()

      Return states with are considered failed.


   .. py:method:: get_sagemaker_response()

      Check status of a SageMaker task.


   .. py:method:: state_from_response(response)

      Extract the state from an AWS response.



.. py:class:: SageMakerAutoMLSensor(*, job_name, **kwargs)


   Bases: :py:obj:`SageMakerBaseSensor`

   Poll the auto ML job until it reaches a terminal state; raise AirflowException with the failure reason.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:SageMakerAutoMLSensor`

   :param job_name: unique name of the AutoML job to watch.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_name',)



   .. py:method:: non_terminal_states()

      Return states with should not terminate.


   .. py:method:: failed_states()

      Return states with are considered failed.


   .. py:method:: get_sagemaker_response()

      Check status of a SageMaker task.


   .. py:method:: state_from_response(response)

      Extract the state from an AWS response.
