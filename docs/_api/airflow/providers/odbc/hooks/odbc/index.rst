:mod:`airflow.providers.odbc.hooks.odbc`
========================================

.. py:module:: airflow.providers.odbc.hooks.odbc

.. autoapi-nested-parse::

   This module contains ODBC hook.



Module Contents
---------------

.. py:class:: OdbcHook(*args, database: Optional[str] = None, driver: Optional[str] = None, dsn: Optional[str] = None, connect_kwargs: Optional[dict] = None, sqlalchemy_scheme: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with odbc data sources using pyodbc.

   See :ref:`howto/connection/odbc` for full documentation.

   .. attribute:: DEFAULT_SQLALCHEMY_SCHEME
      :annotation: = mssql+pyodbc

      

   .. attribute:: conn_name_attr
      :annotation: = odbc_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = odbc_default

      

   .. attribute:: supports_autocommit
      :annotation: = True

      

   .. attribute:: connection
      

      ``airflow.Connection`` object with connection id ``odbc_conn_id``


   .. attribute:: database
      

      Database provided in init if exists; otherwise, ``schema`` from ``Connection`` object.


   .. attribute:: sqlalchemy_scheme
      

      Database provided in init if exists; otherwise, ``schema`` from ``Connection`` object.


   .. attribute:: connection_extra_lower
      

      ``connection.extra_dejson`` but where keys are converted to lower case.

      This is used internally for case-insensitive access of odbc params.


   .. attribute:: driver
      

      Driver from init param if given; else try to find one in connection extra.


   .. attribute:: dsn
      

      DSN from init param if given; else try to find one in connection extra.


   .. attribute:: odbc_connection_string
      

      ODBC connection string
      We build connection string instead of using ``pyodbc.connect`` params because, for example, there is
      no param representing ``ApplicationIntent=ReadOnly``.  Any key-value pairs provided in
      ``Connection.extra`` will be added to the connection string.


   .. attribute:: connect_kwargs
      

      Returns effective kwargs to be passed to ``pyodbc.connect`` after merging between conn extra,
      ``connect_kwargs`` and hook init.

      Hook ``connect_kwargs`` precedes ``connect_kwargs`` from conn extra.

      String values for 'true' and 'false' are converted to bool type.

      If ``attrs_before`` provided, keys and values are converted to int, as required by pyodbc.


   
   .. method:: get_conn(self)

      Returns a pyodbc connection object.



   
   .. method:: get_uri(self)

      URI invoked in :py:meth:`~airflow.hooks.dbapi_hook.DbApiHook.get_sqlalchemy_engine` method



   
   .. method:: get_sqlalchemy_connection(self, connect_kwargs: Optional[dict] = None, engine_kwargs: Optional[dict] = None)

      Sqlalchemy connection object




