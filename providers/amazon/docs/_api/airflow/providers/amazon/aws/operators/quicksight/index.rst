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

:py:mod:`airflow.providers.amazon.aws.operators.quicksight`
===========================================================

.. py:module:: airflow.providers.amazon.aws.operators.quicksight


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.quicksight.QuickSightCreateIngestionOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.quicksight.DEFAULT_CONN_ID


.. py:data:: DEFAULT_CONN_ID
   :value: 'aws_default'



.. py:class:: QuickSightCreateIngestionOperator(data_set_id, ingestion_id, ingestion_type = 'FULL_REFRESH', wait_for_completion = True, check_interval = 30, aws_conn_id = DEFAULT_CONN_ID, region = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates and starts a new SPICE ingestion for a dataset;  also helps to Refresh existing SPICE datasets.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:QuickSightCreateIngestionOperator`

   :param data_set_id:  ID of the dataset used in the ingestion.
   :param ingestion_id: ID for the ingestion.
   :param ingestion_type: Type of ingestion. Values Can be  INCREMENTAL_REFRESH or FULL_REFRESH.
       Default FULL_REFRESH.
   :param wait_for_completion: If wait is set to True, the time interval, in seconds,
       that the operation waits to check the status of the Amazon QuickSight Ingestion.
   :param check_interval: if wait is set to be true, this is the time interval
       in seconds which the operator will check the status of the Amazon QuickSight Ingestion
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).
   :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('data_set_id', 'ingestion_id', 'ingestion_type', 'wait_for_completion', 'check_interval',...



   .. py:attribute:: ui_color
      :value: '#ffd700'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
