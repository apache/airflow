:mod:`airflow.providers.amazon.aws.transfers.dynamodb_to_s3`
============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.dynamodb_to_s3

.. autoapi-nested-parse::

   This module contains operators to replicate records from
   DynamoDB table to S3.



Module Contents
---------------

.. function:: _convert_item_to_json_bytes(item: Dict[str, Any]) -> bytes

.. function:: _upload_file_to_s3(file_obj: IO, bucket_name: str, s3_key_prefix: str) -> None

.. py:class:: DynamoDBToS3Operator(*, dynamodb_table_name: str, s3_bucket_name: str, file_size: int, dynamodb_scan_kwargs: Optional[Dict[str, Any]] = None, s3_key_prefix: str = '', process_func: Callable[[Dict[str, Any]], bytes] = _convert_item_to_json_bytes, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Replicates records from a DynamoDB table to S3.
   It scans a DynamoDB table and write the received records to a file
   on the local filesystem. It flushes the file to S3 once the file size
   exceeds the file size limit specified by the user.

   Users can also specify a filtering criteria using dynamodb_scan_kwargs
   to only replicate records that satisfy the criteria.

   To parallelize the replication, users can create multiple tasks of DynamoDBToS3Operator.
   For instance to replicate with parallelism of 2, create two tasks like:

   .. code-block::

       op1 = DynamoDBToS3Operator(
           task_id='replicator-1',
           dynamodb_table_name='hello',
           dynamodb_scan_kwargs={
               'TotalSegments': 2,
               'Segment': 0,
           },
           ...
       )

       op2 = DynamoDBToS3Operator(
           task_id='replicator-2',
           dynamodb_table_name='hello',
           dynamodb_scan_kwargs={
               'TotalSegments': 2,
               'Segment': 1,
           },
           ...
       )

   :param dynamodb_table_name: Dynamodb table to replicate data from
   :param s3_bucket_name: S3 bucket to replicate data to
   :param file_size: Flush file to s3 if file size >= file_size
   :param dynamodb_scan_kwargs: kwargs pass to <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan>  # noqa: E501 pylint: disable=line-too-long
   :param s3_key_prefix: Prefix of s3 object key
   :param process_func: How we transforms a dynamodb item to bytes. By default we dump the json

   
   .. method:: execute(self, context)



   
   .. method:: _scan_dynamodb_and_upload_to_s3(self, temp_file: IO, scan_kwargs: dict, table: Any)




