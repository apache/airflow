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

:py:mod:`airflow.providers.amazon.aws.operators.appflow`
========================================================

.. py:module:: airflow.providers.amazon.aws.operators.appflow


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.appflow.AppflowBaseOperator
   airflow.providers.amazon.aws.operators.appflow.AppflowRunOperator
   airflow.providers.amazon.aws.operators.appflow.AppflowRunFullOperator
   airflow.providers.amazon.aws.operators.appflow.AppflowRunBeforeOperator
   airflow.providers.amazon.aws.operators.appflow.AppflowRunAfterOperator
   airflow.providers.amazon.aws.operators.appflow.AppflowRunDailyOperator
   airflow.providers.amazon.aws.operators.appflow.AppflowRecordsShortCircuitOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.appflow.SUPPORTED_SOURCES
   airflow.providers.amazon.aws.operators.appflow.MANDATORY_FILTER_DATE_MSG
   airflow.providers.amazon.aws.operators.appflow.NOT_SUPPORTED_SOURCE_MSG


.. py:data:: SUPPORTED_SOURCES



.. py:data:: MANDATORY_FILTER_DATE_MSG
   :value: 'The filter_date argument is mandatory for {entity}!'



.. py:data:: NOT_SUPPORTED_SOURCE_MSG
   :value: 'Source {source} is not supported for {entity}!'



.. py:class:: AppflowBaseOperator(flow_name, flow_update, source = None, source_field = None, filter_date = None, poll_interval = 20, max_attempts = 60, aws_conn_id = 'aws_default', region = None, wait_for_completion = True, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Amazon Appflow Base Operator class (not supposed to be used directly in DAGs).

   :param source: The source name (Supported: salesforce, zendesk)
   :param flow_name: The flow name
   :param flow_update: A boolean to enable/disable a flow update before the run
   :param source_field: The field name to apply filters
   :param filter_date: The date value (or template) to be used in filters.
   :param poll_interval: how often in seconds to check the query status
   :param max_attempts: how many times to check for status before timing out
   :param aws_conn_id: aws connection to use
   :param region: aws region to use
   :param wait_for_completion: whether to wait for the run to end to return

   .. py:attribute:: ui_color
      :value: '#2bccbd'



   .. py:attribute:: template_fields
      :value: ('flow_name', 'source', 'source_field', 'filter_date')



   .. py:attribute:: UPDATE_PROPAGATION_TIME
      :type: int
      :value: 15



   .. py:method:: hook()

      Create and return an AppflowHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: AppflowRunOperator(flow_name, source = None, poll_interval = 20, aws_conn_id = 'aws_default', region = None, wait_for_completion = True, **kwargs)


   Bases: :py:obj:`AppflowBaseOperator`

   Execute a Appflow run as is.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AppflowRunOperator`

   :param source: Obsolete, unnecessary for this operator
   :param flow_name: The flow name
   :param poll_interval: how often in seconds to check the query status
   :param aws_conn_id: aws connection to use
   :param region: aws region to use
   :param wait_for_completion: whether to wait for the run to end to return


.. py:class:: AppflowRunFullOperator(source, flow_name, poll_interval = 20, aws_conn_id = 'aws_default', region = None, wait_for_completion = True, **kwargs)


   Bases: :py:obj:`AppflowBaseOperator`

   Execute a Appflow full run removing any filter.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AppflowRunFullOperator`

   :param source: The source name (Supported: salesforce, zendesk)
   :param flow_name: The flow name
   :param poll_interval: how often in seconds to check the query status
   :param aws_conn_id: aws connection to use
   :param region: aws region to use
   :param wait_for_completion: whether to wait for the run to end to return


.. py:class:: AppflowRunBeforeOperator(source, flow_name, source_field, filter_date, poll_interval = 20, aws_conn_id = 'aws_default', region = None, wait_for_completion = True, **kwargs)


   Bases: :py:obj:`AppflowBaseOperator`

   Execute a Appflow run after updating the filters to select only previous data.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AppflowRunBeforeOperator`

   :param source: The source name (Supported: salesforce)
   :param flow_name: The flow name
   :param source_field: The field name to apply filters
   :param filter_date: The date value (or template) to be used in filters.
   :param poll_interval: how often in seconds to check the query status
   :param aws_conn_id: aws connection to use
   :param region: aws region to use
   :param wait_for_completion: whether to wait for the run to end to return


.. py:class:: AppflowRunAfterOperator(source, flow_name, source_field, filter_date, poll_interval = 20, aws_conn_id = 'aws_default', region = None, wait_for_completion = True, **kwargs)


   Bases: :py:obj:`AppflowBaseOperator`

   Execute a Appflow run after updating the filters to select only future data.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AppflowRunAfterOperator`

   :param source: The source name (Supported: salesforce, zendesk)
   :param flow_name: The flow name
   :param source_field: The field name to apply filters
   :param filter_date: The date value (or template) to be used in filters.
   :param poll_interval: how often in seconds to check the query status
   :param aws_conn_id: aws connection to use
   :param region: aws region to use
   :param wait_for_completion: whether to wait for the run to end to return


.. py:class:: AppflowRunDailyOperator(source, flow_name, source_field, filter_date, poll_interval = 20, aws_conn_id = 'aws_default', region = None, wait_for_completion = True, **kwargs)


   Bases: :py:obj:`AppflowBaseOperator`

   Execute a Appflow run after updating the filters to select only a single day.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AppflowRunDailyOperator`

   :param source: The source name (Supported: salesforce)
   :param flow_name: The flow name
   :param source_field: The field name to apply filters
   :param filter_date: The date value (or template) to be used in filters.
   :param poll_interval: how often in seconds to check the query status
   :param aws_conn_id: aws connection to use
   :param region: aws region to use
   :param wait_for_completion: whether to wait for the run to end to return


.. py:class:: AppflowRecordsShortCircuitOperator(*, flow_name, appflow_run_task_id, ignore_downstream_trigger_rules = True, aws_conn_id = 'aws_default', region = None, **kwargs)


   Bases: :py:obj:`airflow.operators.python.ShortCircuitOperator`

   Short-circuit in case of a empty Appflow's run.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AppflowRecordsShortCircuitOperator`

   :param flow_name: The flow name
   :param appflow_run_task_id: Run task ID from where this operator should extract the execution ID
   :param ignore_downstream_trigger_rules: Ignore downstream trigger rules
   :param aws_conn_id: aws connection to use
   :param region: aws region to use

   .. py:attribute:: ui_color
      :value: '#33ffec'



   .. py:method:: hook()

      Create and return an AppflowHook.
