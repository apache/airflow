:mod:`airflow.providers.salesforce.hooks.salesforce`
====================================================

.. py:module:: airflow.providers.salesforce.hooks.salesforce

.. autoapi-nested-parse::

   This module contains a Salesforce Hook which allows you to connect to your Salesforce instance,
   retrieve data from it, and write that data to a file for other uses.

   .. note:: this hook also relies on the simple_salesforce package:
         https://github.com/simple-salesforce/simple-salesforce



Module Contents
---------------

.. data:: log
   

   

.. py:class:: SalesforceHook(conn_id: str)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Create new connection to Salesforce and allows you to pull data out of SFDC and save it to a file.

   You can then use that file with other Airflow operators to move the data into another data source.

   :param conn_id: the name of the connection that has the parameters we need to connect to Salesforce.
       The connection should be type `http` and include a user's security token in the `Extras` field.
   :type conn_id: str

   .. note::
       For the HTTP connection type, you can include a
       JSON structure in the `Extras` field.
       We need a user's security token to connect to Salesforce.
       So we define it in the `Extras` field as `{"security_token":"YOUR_SECURITY_TOKEN"}`

       For sandbox mode, add `{"domain":"test"}` in the `Extras` field

   
   .. method:: get_conn(self)

      Sign into Salesforce, only if we are not already signed in.



   
   .. method:: make_query(self, query: str, include_deleted: bool = False, query_params: Optional[dict] = None)

      Make a query to Salesforce.

      :param query: The query to make to Salesforce.
      :type query: str
      :param include_deleted: True if the query should include deleted records.
      :type include_deleted: bool
      :param query_params: Additional optional arguments
      :type query_params: dict
      :return: The query result.
      :rtype: dict



   
   .. method:: describe_object(self, obj: str)

      Get the description of an object from Salesforce.
      This description is the object's schema and
      some extra metadata that Salesforce stores for each object.

      :param obj: The name of the Salesforce object that we are getting a description of.
      :type obj: str
      :return: the description of the Salesforce object.
      :rtype: dict



   
   .. method:: get_available_fields(self, obj: str)

      Get a list of all available fields for an object.

      :param obj: The name of the Salesforce object that we are getting a description of.
      :type obj: str
      :return: the names of the fields.
      :rtype: list(str)



   
   .. method:: get_object_from_salesforce(self, obj: str, fields: Iterable[str])

      Get all instances of the `object` from Salesforce.
      For each model, only get the fields specified in fields.

      All we really do underneath the hood is run:
          SELECT <fields> FROM <obj>;

      :param obj: The object name to get from Salesforce.
      :type obj: str
      :param fields: The fields to get from the object.
      :type fields: iterable
      :return: all instances of the object from Salesforce.
      :rtype: dict



   
   .. classmethod:: _to_timestamp(cls, column: pd.Series)

      Convert a column of a dataframe to UNIX timestamps if applicable

      :param column: A Series object representing a column of a dataframe.
      :type column: pandas.Series
      :return: a new series that maintains the same index as the original
      :rtype: pandas.Series



   
   .. method:: write_object_to_file(self, query_results: List[dict], filename: str, fmt: str = 'csv', coerce_to_timestamp: bool = False, record_time_added: bool = False)

      Write query results to file.

      Acceptable formats are:
          - csv:
              comma-separated-values file. This is the default format.
          - json:
              JSON array. Each element in the array is a different row.
          - ndjson:
              JSON array but each element is new-line delimited instead of comma delimited like in `json`

      This requires a significant amount of cleanup.
      Pandas doesn't handle output to CSV and json in a uniform way.
      This is especially painful for datetime types.
      Pandas wants to write them as strings in CSV, but as millisecond Unix timestamps.

      By default, this function will try and leave all values as they are represented in Salesforce.
      You use the `coerce_to_timestamp` flag to force all datetimes to become Unix timestamps (UTC).
      This is can be greatly beneficial as it will make all of your datetime fields look the same,
      and makes it easier to work with in other database environments

      :param query_results: the results from a SQL query
      :type query_results: list of dict
      :param filename: the name of the file where the data should be dumped to
      :type filename: str
      :param fmt: the format you want the output in. Default:  'csv'
      :type fmt: str
      :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
          False if you want them to be left in the same format as they were in Salesforce.
          Leaving the value as False will result in datetimes being strings. Default: False
      :type coerce_to_timestamp: bool
      :param record_time_added: True if you want to add a Unix timestamp field
          to the resulting data that marks when the data was fetched from Salesforce. Default: False
      :type record_time_added: bool
      :return: the dataframe that gets written to the file.
      :rtype: pandas.Dataframe



   
   .. method:: object_to_df(self, query_results: List[dict], coerce_to_timestamp: bool = False, record_time_added: bool = False)

      Export query results to dataframe.

      By default, this function will try and leave all values as they are represented in Salesforce.
      You use the `coerce_to_timestamp` flag to force all datetimes to become Unix timestamps (UTC).
      This is can be greatly beneficial as it will make all of your datetime fields look the same,
      and makes it easier to work with in other database environments

      :param query_results: the results from a SQL query
      :type query_results: list of dict
      :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
          False if you want them to be left in the same format as they were in Salesforce.
          Leaving the value as False will result in datetimes being strings. Default: False
      :type coerce_to_timestamp: bool
      :param record_time_added: True if you want to add a Unix timestamp field
          to the resulting data that marks when the data was fetched from Salesforce. Default: False
      :type record_time_added: bool
      :return: the dataframe.
      :rtype: pandas.Dataframe




