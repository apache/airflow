:mod:`airflow.api.client.json_client`
=====================================

.. py:module:: airflow.api.client.json_client

.. autoapi-nested-parse::

   JSON API Client



Module Contents
---------------

.. py:class:: Client

   Bases: :class:`airflow.api.client.api_client.Client`

   Json API client implementation.

   
   .. method:: _request(self, url, method='GET', json=None)



   
   .. method:: trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None)



   
   .. method:: delete_dag(self, dag_id)



   
   .. method:: get_pool(self, name)



   
   .. method:: get_pools(self)



   
   .. method:: create_pool(self, name, slots, description)



   
   .. method:: delete_pool(self, name)



   
   .. method:: get_lineage(self, dag_id: str, execution_date: str)




