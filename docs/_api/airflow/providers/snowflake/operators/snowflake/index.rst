:mod:`airflow.providers.snowflake.operators.snowflake`
======================================================

.. py:module:: airflow.providers.snowflake.operators.snowflake


Module Contents
---------------

.. py:class:: SnowflakeOperator(*, sql: Any, snowflake_conn_id: str = 'snowflake_default', parameters: Optional[dict] = None, autocommit: bool = True, warehouse: Optional[str] = None, database: Optional[str] = None, role: Optional[str] = None, schema: Optional[str] = None, authenticator: Optional[str] = None, session_parameters: Optional[dict] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes sql code in a Snowflake database

   :param snowflake_conn_id: reference to specific snowflake connection id
   :type snowflake_conn_id: str
   :param sql: the sql code to be executed. (templated)
   :type sql: Can receive a str representing a sql statement,
       a list of str (sql statements), or reference to a template file.
       Template reference are recognized by str ending in '.sql'
   :param autocommit: if True, each command is automatically committed.
       (default value: True)
   :type autocommit: bool
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: dict or iterable
   :param warehouse: name of warehouse (will overwrite any warehouse
       defined in the connection's extra JSON)
   :type warehouse: str
   :param database: name of database (will overwrite database defined
       in connection)
   :type database: str
   :param schema: name of schema (will overwrite schema defined in
       connection)
   :type schema: str
   :param role: name of role (will overwrite any role defined in
       connection's extra JSON)
   :type role: str
   :param authenticator: authenticator for Snowflake.
       'snowflake' (default) to use the internal Snowflake authenticator
       'externalbrowser' to authenticate using your web browser and
       Okta, ADFS or any other SAML 2.0-compliant identify provider
       (IdP) that has been defined for your account
       'https://<your_okta_account_name>.okta.com' to authenticate
       through native Okta.
   :type authenticator: str
   :param session_parameters: You can set session-level parameters at
       the time you connect to Snowflake
   :type session_parameters: dict

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: get_hook(self)

      Create and return SnowflakeHook.
      :return: a SnowflakeHook instance.
      :rtype: SnowflakeHook



   
   .. method:: execute(self, context: Any)

      Run query on snowflake




