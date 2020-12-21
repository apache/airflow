:mod:`airflow.providers.snowflake.transfers.snowflake_to_slack`
===============================================================

.. py:module:: airflow.providers.snowflake.transfers.snowflake_to_slack


Module Contents
---------------

.. py:class:: SnowflakeToSlackOperator(*, sql: str, slack_message: str, snowflake_conn_id: str = 'snowflake_default', slack_conn_id: str = 'slack_default', results_df_name: str = 'results_df', parameters: Optional[Union[Iterable, Mapping]] = None, warehouse: Optional[str] = None, database: Optional[str] = None, schema: Optional[str] = None, role: Optional[str] = None, slack_token: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes an SQL statement in Snowflake and sends the results to Slack. The results of the query are
   rendered into the 'slack_message' parameter as a Pandas dataframe using a JINJA variable called '{{
   results_df }}'. The 'results_df' variable name can be changed by specifying a different
   'results_df_name' parameter. The Tabulate library is added to the JINJA environment as a filter to
   allow the dataframe to be rendered nicely. For example, set 'slack_message' to {{ results_df |
   tabulate(tablefmt="pretty", headers="keys") }} to send the results to Slack as an ascii rendered table.

   :param sql: The SQL statement to execute on Snowflake (templated)
   :type sql: str
   :param slack_message: The templated Slack message to send with the data returned from Snowflake.
       You can use the default JINJA variable {{ results_df }} to access the pandas dataframe containing the
       SQL results
   :type slack_message: str
   :param snowflake_conn_id: The Snowflake connection id
   :type snowflake_conn_id: str
   :param slack_conn_id: The connection id for Slack
   :type slack_conn_id: str
   :param results_df_name: The name of the JINJA template's dataframe variable, default is 'results_df'
   :type results_df_name: str
   :param parameters: The parameters to pass to the SQL query
   :type parameters: Optional[Union[Iterable, Mapping]]
   :param warehouse: The Snowflake virtual warehouse to use to run the SQL query
   :type warehouse: Optional[str]
   :param database: The Snowflake database to use for the SQL query
   :type database: Optional[str]
   :param schema: The schema to run the SQL against in Snowflake
   :type schema: Optional[str]
   :param role: The role to use when connecting to Snowflake
   :type role: Optional[str]
   :param slack_token: The token to use to authenticate to Slack. If this is not provided, the
       'webhook_token' attribute needs to be specified in the 'Extra' JSON field against the slack_conn_id
   :type slack_token: Optional[str]

   .. attribute:: template_fields
      :annotation: = ['sql', 'slack_message']

      

   .. attribute:: template_ext
      :annotation: = ['.sql', '.jinja', '.j2']

      

   .. attribute:: times_rendered
      :annotation: = 0

      

   
   .. method:: _get_query_results(self)



   
   .. method:: _render_and_send_slack_message(self, context, df)



   
   .. method:: _get_snowflake_hook(self)



   
   .. method:: _get_slack_hook(self)



   
   .. method:: render_template_fields(self, context, jinja_env=None)



   
   .. method:: execute(self, context)




