:mod:`airflow.providers.postgres.hooks.postgres`
================================================

.. py:module:: airflow.providers.postgres.hooks.postgres


Module Contents
---------------

.. data:: CursorType
   

   

.. py:class:: PostgresHook(*args, **kwargs)

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Postgres.

   You can specify ssl parameters in the extra field of your connection
   as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.
   Also you can choose cursor as ``{"cursor": "dictcursor"}``. Refer to the
   psycopg2.extras for more details.

   Note: For Redshift, use keepalives_idle in the extra connection parameters
   and set it to less than 300 seconds.

   Note: For AWS IAM authentication, use iam in the extra connection parameters
   and set it to true. Leave the password field empty. This will use the
   "aws_default" connection to get the temporary token unless you override
   in extras.
   extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``
   For Redshift, also use redshift in the extra connection parameters and
   set it to true. The cluster-identifier is extracted from the beginning of
   the host field, so is optional. It can however be overridden in the extra field.
   extras example: ``{"iam":true, "redshift":true, "cluster-identifier": "my_cluster_id"}``

   .. attribute:: conn_name_attr
      :annotation: = postgres_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = postgres_default

      

   .. attribute:: supports_autocommit
      :annotation: = True

      

   
   .. method:: _get_cursor(self, raw_cursor: str)



   
   .. method:: get_conn(self)

      Establishes a connection to a postgres database.



   
   .. method:: copy_expert(self, sql: str, filename: str)

      Executes SQL using psycopg2 copy_expert method.
      Necessary to execute COPY command without access to a superuser.

      Note: if this method is called with a "COPY FROM" statement and
      the specified input file does not exist, it creates an empty
      file and no data is loaded, but the operation succeeds.
      So if users want to be aware when the input file does not exist,
      they have to check its existence by themselves.



   
   .. method:: bulk_load(self, table: str, tmp_file: str)

      Loads a tab-delimited file into a database table



   
   .. method:: bulk_dump(self, table: str, tmp_file: str)

      Dumps a database table into a tab-delimited file



   
   .. staticmethod:: _serialize_cell(cell: object, conn: Optional[connection] = None)

      Postgresql will adapt all arguments to the execute() method internally,
      hence we return cell without any conversion.

      See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
      more information.

      :param cell: The cell to insert into the table
      :type cell: object
      :param conn: The database connection
      :type conn: connection object
      :return: The cell
      :rtype: object



   
   .. method:: get_iam_token(self, conn: Connection)

      Uses AWSHook to retrieve a temporary password to connect to Postgres
      or Redshift. Port is required. If none is provided, default is used for
      each service



   
   .. staticmethod:: _generate_insert_sql(table: str, values: Tuple[str, ...], target_fields: Iterable[str], replace: bool, **kwargs)

      Static helper method that generate the INSERT SQL statement.
      The REPLACE variant is specific to MySQL syntax.

      :param table: Name of the target table
      :type table: str
      :param values: The row to insert into the table
      :type values: tuple of cell values
      :param target_fields: The names of the columns to fill in the table
      :type target_fields: iterable of strings
      :param replace: Whether to replace instead of insert
      :type replace: bool
      :param replace_index: the column or list of column names to act as
          index for the ON CONFLICT clause
      :type replace_index: str or list
      :return: The generated INSERT or REPLACE SQL statement
      :rtype: str




