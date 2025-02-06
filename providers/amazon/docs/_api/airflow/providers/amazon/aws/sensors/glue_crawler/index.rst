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

:py:mod:`airflow.providers.amazon.aws.sensors.glue_crawler`
===========================================================

.. py:module:: airflow.providers.amazon.aws.sensors.glue_crawler


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.glue_crawler.GlueCrawlerSensor




.. py:class:: GlueCrawlerSensor(*, crawler_name, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Waits for an AWS Glue crawler to reach any of the statuses below.

   'FAILED', 'CANCELLED', 'SUCCEEDED'

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:GlueCrawlerSensor`

   :param crawler_name: The AWS Glue crawler unique name
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('crawler_name',)



   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: get_hook()

      Return a new or pre-existing GlueCrawlerHook.


   .. py:method:: hook()
