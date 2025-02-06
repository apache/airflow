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

:py:mod:`airflow.providers.amazon.aws.sensors.glue`
===================================================

.. py:module:: airflow.providers.amazon.aws.sensors.glue


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.glue.GlueJobSensor




.. py:class:: GlueJobSensor(*, job_name, run_id, verbose = False, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Waits for an AWS Glue Job to reach any of the status below.

   'FAILED', 'STOPPED', 'SUCCEEDED'

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:GlueJobSensor`

   :param job_name: The AWS Glue Job unique name
   :param run_id: The AWS Glue current running job identifier
   :param verbose: If True, more Glue Job Run logs show in the Airflow Task Logs.  (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('job_name', 'run_id')



   .. py:method:: hook()


   .. py:method:: poke(context)

      Override when deriving this class.
