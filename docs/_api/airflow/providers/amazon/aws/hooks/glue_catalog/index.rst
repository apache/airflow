:mod:`airflow.providers.amazon.aws.hooks.glue_catalog`
======================================================

.. py:module:: airflow.providers.amazon.aws.hooks.glue_catalog

.. autoapi-nested-parse::

   This module contains AWS Glue Catalog Hook



Module Contents
---------------

.. py:class:: AwsGlueCatalogHook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Glue Catalog

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: get_partitions(self, database_name: str, table_name: str, expression: str = '', page_size: Optional[int] = None, max_items: Optional[int] = None)

      Retrieves the partition values for a table.

      :param database_name: The name of the catalog database where the partitions reside.
      :type database_name: str
      :param table_name: The name of the partitions' table.
      :type table_name: str
      :param expression: An expression filtering the partitions to be returned.
          Please see official AWS documentation for further information.
          https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetPartitions
      :type expression: str
      :param page_size: pagination size
      :type page_size: int
      :param max_items: maximum items to return
      :type max_items: int
      :return: set of partition values where each value is a tuple since
          a partition may be composed of multiple columns. For example:
          ``{('2018-01-01','1'), ('2018-01-01','2')}``



   
   .. method:: check_for_partition(self, database_name: str, table_name: str, expression: str)

      Checks whether a partition exists

      :param database_name: Name of hive database (schema) @table belongs to
      :type database_name: str
      :param table_name: Name of hive table @partition belongs to
      :type table_name: str
      :expression: Expression that matches the partitions to check for
          (eg `a = 'b' AND c = 'd'`)
      :type expression: str
      :rtype: bool

      >>> hook = AwsGlueCatalogHook()
      >>> t = 'static_babynames_partitioned'
      >>> hook.check_for_partition('airflow', t, "ds='2015-01-01'")
      True



   
   .. method:: get_table(self, database_name: str, table_name: str)

      Get the information of the table

      :param database_name: Name of hive database (schema) @table belongs to
      :type database_name: str
      :param table_name: Name of hive table
      :type table_name: str
      :rtype: dict

      >>> hook = AwsGlueCatalogHook()
      >>> r = hook.get_table('db', 'table_foo')
      >>> r['Name'] = 'table_foo'



   
   .. method:: get_table_location(self, database_name: str, table_name: str)

      Get the physical location of the table

      :param database_name: Name of hive database (schema) @table belongs to
      :type database_name: str
      :param table_name: Name of hive table
      :type table_name: str
      :return: str




