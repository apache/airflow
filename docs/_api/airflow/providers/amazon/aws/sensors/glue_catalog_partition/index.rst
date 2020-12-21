:mod:`airflow.providers.amazon.aws.sensors.glue_catalog_partition`
==================================================================

.. py:module:: airflow.providers.amazon.aws.sensors.glue_catalog_partition


Module Contents
---------------

.. py:class:: AwsGlueCatalogPartitionSensor(*, table_name: str, expression: str = "ds='{{ ds }}'", aws_conn_id: str = 'aws_default', region_name: Optional[str] = None, database_name: str = 'default', poke_interval: int = 60 * 3, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a partition to show up in AWS Glue Catalog.

   :param table_name: The name of the table to wait for, supports the dot
       notation (my_database.my_table)
   :type table_name: str
   :param expression: The partition clause to wait for. This is passed as
       is to the AWS Glue Catalog API's get_partitions function,
       and supports SQL like notation as in ``ds='2015-01-01'
       AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``.
       See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html
       #aws-glue-api-catalog-partitions-GetPartitions
   :type expression: str
   :param aws_conn_id: ID of the Airflow connection where
       credentials and extra configuration are stored
   :type aws_conn_id: str
   :param region_name: Optional aws region name (example: us-east-1). Uses region from connection
       if not specified.
   :type region_name: str
   :param database_name: The name of the catalog database where the partitions reside.
   :type database_name: str
   :param poke_interval: Time in seconds that the job should wait in
       between each tries
   :type poke_interval: int

   .. attribute:: template_fields
      :annotation: = ['database_name', 'table_name', 'expression']

      

   .. attribute:: ui_color
      :annotation: = #C5CAE9

      

   
   .. method:: poke(self, context)

      Checks for existence of the partition in the AWS Glue Catalog table



   
   .. method:: get_hook(self)

      Gets the AwsGlueCatalogHook




