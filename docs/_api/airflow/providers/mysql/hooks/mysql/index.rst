:mod:`airflow.providers.mysql.hooks.mysql`
==========================================

.. py:module:: airflow.providers.mysql.hooks.mysql

.. autoapi-nested-parse::

   This module allows to connect to a MySQL database.



Module Contents
---------------

.. py:class:: MySqlHook(*args, **kwargs)

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with MySQL.

   You can specify charset in the extra field of your connection
   as ``{"charset": "utf8"}``. Also you can choose cursor as
   ``{"cursor": "SSCursor"}``. Refer to the MySQLdb.cursors for more details.

   Note: For AWS IAM authentication, use iam in the extra connection parameters
   and set it to true. Leave the password field empty. This will use the
   "aws_default" connection to get the temporary token unless you override
   in extras.
   extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``

   .. attribute:: conn_name_attr
      :annotation: = mysql_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = mysql_default

      

   .. attribute:: supports_autocommit
      :annotation: = True

      

   
   .. method:: set_autocommit(self, conn: Connection, autocommit: bool)

      MySql connection sets autocommit in a different way.



   
   .. method:: get_autocommit(self, conn: Connection)

      MySql connection gets autocommit in a different way.

      :param conn: connection to get autocommit setting from.
      :type conn: connection object.
      :return: connection autocommit setting
      :rtype: bool



   
   .. method:: _get_conn_config_mysql_client(self, conn: Connection)



   
   .. method:: _get_conn_config_mysql_connector_python(self, conn: Connection)



   
   .. method:: get_conn(self)

      Establishes a connection to a mysql database
      by extracting the connection configuration from the Airflow connection.

      .. note:: By default it connects to the database via the mysqlclient library.
          But you can also choose the mysql-connector-python library which lets you connect through ssl
          without any further ssl parameters required.

      :return: a mysql connection object



   
   .. method:: get_uri(self)



   
   .. method:: bulk_load(self, table: str, tmp_file: str)

      Loads a tab-delimited file into a database table



   
   .. method:: bulk_dump(self, table: str, tmp_file: str)

      Dumps a database table into a tab-delimited file



   
   .. staticmethod:: _serialize_cell(cell: object, conn: Optional[Connection] = None)

      MySQLdb converts an argument to a literal
      when passing those separately to execute. Hence, this method does nothing.

      :param cell: The cell to insert into the table
      :type cell: object
      :param conn: The database connection
      :type conn: connection object
      :return: The same cell
      :rtype: object



   
   .. method:: get_iam_token(self, conn: Connection)

      Uses AWSHook to retrieve a temporary password to connect to MySQL
      Port is required. If none is provided, default 3306 is used



   
   .. method:: bulk_load_custom(self, table: str, tmp_file: str, duplicate_key_handling: str = 'IGNORE', extra_options: str = '')

      A more configurable way to load local data from a file into the database.

      .. warning:: According to the mysql docs using this function is a
          `security risk <https://dev.mysql.com/doc/refman/8.0/en/load-data-local.html>`_.
          If you want to use it anyway you can do so by setting a client-side + server-side option.
          This depends on the mysql client library used.

      :param table: The table were the file will be loaded into.
      :type table: str
      :param tmp_file: The file (name) that contains the data.
      :type tmp_file: str
      :param duplicate_key_handling: Specify what should happen to duplicate data.
          You can choose either `IGNORE` or `REPLACE`.

          .. seealso::
              https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-duplicate-key-handling
      :type duplicate_key_handling: str
      :param extra_options: More sql options to specify exactly how to load the data.

          .. seealso:: https://dev.mysql.com/doc/refman/8.0/en/load-data.html
      :type extra_options: str




