:mod:`airflow.providers.amazon.aws.hooks.dynamodb`
==================================================

.. py:module:: airflow.providers.amazon.aws.hooks.dynamodb

.. autoapi-nested-parse::

   This module contains the AWS DynamoDB hook



Module Contents
---------------

.. py:class:: AwsDynamoDBHook(*args, table_keys: Optional[List] = None, table_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS DynamoDB.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   :param table_keys: partition key and sort key
   :type table_keys: list
   :param table_name: target DynamoDB table
   :type table_name: str

   
   .. method:: write_batch_data(self, items: Iterable)

      Write batch items to DynamoDB table with provisioned throughout capacity.




