:mod:`airflow.providers.apache.hive.transfers.s3_to_hive`
=========================================================

.. py:module:: airflow.providers.apache.hive.transfers.s3_to_hive

.. autoapi-nested-parse::

   This module contains operator to move data from Hive to S3 bucket.



Module Contents
---------------

.. py:class:: S3ToHiveOperator(*, s3_key: str, field_dict: Dict, hive_table: str, delimiter: str = ',', create: bool = True, recreate: bool = False, partition: Optional[Dict] = None, headers: bool = False, check_headers: bool = False, wildcard_match: bool = False, aws_conn_id: str = 'aws_default', verify: Optional[Union[bool, str]] = None, hive_cli_conn_id: str = 'hive_cli_default', input_compressed: bool = False, tblproperties: Optional[Dict] = None, select_expression: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from S3 to Hive. The operator downloads a file from S3,
   stores the file locally before loading it into a Hive table.
   If the ``create`` or ``recreate`` arguments are set to ``True``,
   a ``CREATE TABLE`` and ``DROP TABLE`` statements are generated.
   Hive data types are inferred from the cursor's metadata from.

   Note that the table generated in Hive uses ``STORED AS textfile``
   which isn't the most efficient serialization format. If a
   large amount of data is loaded and/or if the tables gets
   queried considerably, you may want to use this operator only to
   stage the data into a temporary table before loading it into its
   final destination using a ``HiveOperator``.

   :param s3_key: The key to be retrieved from S3. (templated)
   :type s3_key: str
   :param field_dict: A dictionary of the fields name in the file
       as keys and their Hive types as values
   :type field_dict: dict
   :param hive_table: target Hive table, use dot notation to target a
       specific database. (templated)
   :type hive_table: str
   :param delimiter: field delimiter in the file
   :type delimiter: str
   :param create: whether to create the table if it doesn't exist
   :type create: bool
   :param recreate: whether to drop and recreate the table at every
       execution
   :type recreate: bool
   :param partition: target partition as a dict of partition columns
       and values. (templated)
   :type partition: dict
   :param headers: whether the file contains column names on the first
       line
   :type headers: bool
   :param check_headers: whether the column names on the first line should be
       checked against the keys of field_dict
   :type check_headers: bool
   :param wildcard_match: whether the s3_key should be interpreted as a Unix
       wildcard pattern
   :type wildcard_match: bool
   :param aws_conn_id: source s3 connection
   :type aws_conn_id: str
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :type verify: bool or str
   :param hive_cli_conn_id: destination hive connection
   :type hive_cli_conn_id: str
   :param input_compressed: Boolean to determine if file decompression is
       required to process headers
   :type input_compressed: bool
   :param tblproperties: TBLPROPERTIES of the hive table being created
   :type tblproperties: dict
   :param select_expression: S3 Select expression
   :type select_expression: str

   .. attribute:: template_fields
      :annotation: = ['s3_key', 'partition', 'hive_table']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   
   .. method:: execute(self, context)



   
   .. method:: _get_top_row_as_list(self, file_name)



   
   .. method:: _match_headers(self, header_list)



   
   .. staticmethod:: _delete_top_row_and_compress(input_file_name, output_file_ext, dest_dir)




