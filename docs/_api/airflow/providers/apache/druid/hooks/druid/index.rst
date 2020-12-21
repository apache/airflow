:mod:`airflow.providers.apache.druid.hooks.druid`
=================================================

.. py:module:: airflow.providers.apache.druid.hooks.druid


Module Contents
---------------

.. py:class:: DruidHook(druid_ingest_conn_id: str = 'druid_ingest_default', timeout: int = 1, max_ingestion_time: Optional[int] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Connection to Druid overlord for ingestion

   To connect to a Druid cluster that is secured with the druid-basic-security
   extension, add the username and password to the druid ingestion connection.

   :param druid_ingest_conn_id: The connection id to the Druid overlord machine
                                which accepts index jobs
   :type druid_ingest_conn_id: str
   :param timeout: The interval between polling
                   the Druid job for the status of the ingestion job.
                   Must be greater than or equal to 1
   :type timeout: int
   :param max_ingestion_time: The maximum ingestion time before assuming the job failed
   :type max_ingestion_time: int

   
   .. method:: get_conn_url(self)

      Get Druid connection url



   
   .. method:: get_auth(self)

      Return username and password from connections tab as requests.auth.HTTPBasicAuth object.

      If these details have not been set then returns None.



   
   .. method:: submit_indexing_job(self, json_index_spec: Dict[str, Any])

      Submit Druid ingestion job




.. py:class:: DruidDbApiHook

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Druid broker

   This hook is purely for users to query druid broker.
   For ingestion, please use druidHook.

   .. attribute:: conn_name_attr
      :annotation: = druid_broker_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = druid_broker_default

      

   .. attribute:: supports_autocommit
      :annotation: = False

      

   
   .. method:: get_conn(self)

      Establish a connection to druid broker.



   
   .. method:: get_uri(self)

      Get the connection uri for druid broker.

      e.g: druid://localhost:8082/druid/v2/sql/



   
   .. method:: set_autocommit(self, conn: connect, autocommit: bool)



   
   .. method:: insert_rows(self, table: str, rows: Iterable[Tuple[str]], target_fields: Optional[Iterable[str]] = None, commit_every: int = 1000, replace: bool = False, **kwargs)




