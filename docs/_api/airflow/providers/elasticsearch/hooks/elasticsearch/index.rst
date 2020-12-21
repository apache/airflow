:mod:`airflow.providers.elasticsearch.hooks.elasticsearch`
==========================================================

.. py:module:: airflow.providers.elasticsearch.hooks.elasticsearch


Module Contents
---------------

.. py:class:: ElasticsearchHook(schema: str = 'http', connection: Optional[AirflowConnection] = None, *args, **kwargs)

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Elasticsearch through the elasticsearch-dbapi.

   .. attribute:: conn_name_attr
      :annotation: = elasticsearch_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = elasticsearch_default

      

   
   .. method:: get_conn(self)

      Returns a elasticsearch connection object



   
   .. method:: get_uri(self)




