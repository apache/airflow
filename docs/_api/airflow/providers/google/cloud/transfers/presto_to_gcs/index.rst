:mod:`airflow.providers.google.cloud.transfers.presto_to_gcs`
=============================================================

.. py:module:: airflow.providers.google.cloud.transfers.presto_to_gcs


Module Contents
---------------

.. py:class:: _PrestoToGCSPrestoCursorAdapter(cursor: PrestoCursor)

   An adapter that adds additional feature to the Presto cursor.

   The implementation of cursor in the prestodb library is not sufficient.
   The following changes have been made:

   * The poke mechanism for row. You can look at the next row without consuming it.
   * The description attribute is available before reading the first row. Thanks to the poke mechanism.
   * the iterator interface has been implemented.

   A detailed description of the class methods is available in
   `PEP-249 <https://www.python.org/dev/peps/pep-0249/>`__.

   .. attribute:: description
      

      This read-only attribute is a sequence of 7-item sequences.

      Each of these sequences contains information describing one result column:

      * ``name``
      * ``type_code``
      * ``display_size``
      * ``internal_size``
      * ``precision``
      * ``scale``
      * ``null_ok``

      The first two items (``name`` and ``type_code``) are mandatory, the other
      five are optional and are set to None if no meaningful values can be provided.


   .. attribute:: rowcount
      

      The read-only attribute specifies the number of rows


   
   .. method:: close(self)

      Close the cursor now



   
   .. method:: execute(self, *args, **kwargs)

      Prepare and execute a database operation (query or command).



   
   .. method:: executemany(self, *args, **kwargs)

      Prepare a database operation (query or command) and then execute it against all parameter
      sequences or mappings found in the sequence seq_of_parameters.



   
   .. method:: peekone(self)

      Return the next row without consuming it.



   
   .. method:: fetchone(self)

      Fetch the next row of a query result set, returning a single sequence, or
      ``None`` when no more data is available.



   
   .. method:: fetchmany(self, size=None)

      Fetch the next set of rows of a query result, returning a sequence of sequences
      (e.g. a list of tuples). An empty sequence is returned when no more rows are available.



   
   .. method:: __next__(self)

      Return the next row from the currently executing SQL statement using the same semantics as
      ``.fetchone()``.  A ``StopIteration`` exception is raised when the result set is exhausted.
      :return:



   
   .. method:: __iter__(self)

      Return self to make cursors compatible to the iteration protocol




.. py:class:: PrestoToGCSOperator(*, presto_conn_id: str = 'presto_default', **kwargs)

   Bases: :class:`airflow.providers.google.cloud.transfers.sql_to_gcs.BaseSQLToGCSOperator`

   Copy data from PrestoDB to Google Cloud Storage in JSON or CSV format.

   :param presto_conn_id: Reference to a specific Presto hook.
   :type presto_conn_id: str

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   .. attribute:: type_map
      

      

   
   .. method:: query(self)

      Queries presto and returns a cursor to the results.



   
   .. method:: field_to_bigquery(self, field)

      Convert presto field type to BigQuery field type.



   
   .. method:: convert_type(self, value, schema_type)

      Do nothing. Presto uses JSON on the transport layer, so types are simple.

      :param value: Presto column value
      :type value: Any
      :param schema_type: BigQuery data type
      :type schema_type: str




