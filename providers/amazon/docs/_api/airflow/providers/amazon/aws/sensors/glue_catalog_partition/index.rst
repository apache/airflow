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

:py:mod:`airflow.providers.amazon.aws.sensors.glue_catalog_partition`
=====================================================================

.. py:module:: airflow.providers.amazon.aws.sensors.glue_catalog_partition


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.glue_catalog_partition.GlueCatalogPartitionSensor




.. py:class:: GlueCatalogPartitionSensor(*, table_name, expression = "ds='{{ ds }}'", aws_conn_id = 'aws_default', region_name = None, database_name = 'default', poke_interval = 60 * 3, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Waits for a partition to show up in AWS Glue Catalog.

   :param table_name: The name of the table to wait for, supports the dot
       notation (my_database.my_table)
   :param expression: The partition clause to wait for. This is passed as
       is to the AWS Glue Catalog API's get_partitions function,
       and supports SQL like notation as in ``ds='2015-01-01'
       AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``.
       See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html
       #aws-glue-api-catalog-partitions-GetPartitions
   :param aws_conn_id: ID of the Airflow connection where
       credentials and extra configuration are stored
   :param region_name: Optional aws region name (example: us-east-1). Uses region from connection
       if not specified.
   :param database_name: The name of the catalog database where the partitions reside.
   :param poke_interval: Time in seconds that the job should wait in
       between each tries
   :param deferrable: If true, then the sensor will wait asynchronously for the partition to
       show up in the AWS Glue Catalog.
       (default: False, but can be overridden in config file by setting default_deferrable to True)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('database_name', 'table_name', 'expression')



   .. py:attribute:: ui_color
      :value: '#C5CAE9'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: poke(context)

      Check for existence of the partition in the AWS Glue Catalog table.


   .. py:method:: execute_complete(context, event = None)


   .. py:method:: get_hook()

      Get the GlueCatalogHook.


   .. py:method:: hook()
