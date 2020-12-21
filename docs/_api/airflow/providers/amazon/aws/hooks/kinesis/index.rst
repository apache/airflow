:mod:`airflow.providers.amazon.aws.hooks.kinesis`
=================================================

.. py:module:: airflow.providers.amazon.aws.hooks.kinesis

.. autoapi-nested-parse::

   This module contains AWS Firehose hook



Module Contents
---------------

.. py:class:: AwsFirehoseHook(delivery_stream: str, *args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Kinesis Firehose.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   :param delivery_stream: Name of the delivery stream
   :type delivery_stream: str

   
   .. method:: put_records(self, records: Iterable)

      Write batch records to Kinesis Firehose




