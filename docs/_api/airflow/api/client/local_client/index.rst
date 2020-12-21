:mod:`airflow.api.client.local_client`
======================================

.. py:module:: airflow.api.client.local_client

.. autoapi-nested-parse::

   Local client API



Module Contents
---------------

.. py:class:: Client

   Bases: :class:`airflow.api.client.api_client.Client`

   Local API client implementation.

   
   .. method:: trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None)



   
   .. method:: delete_dag(self, dag_id)



   
   .. method:: get_pool(self, name)



   
   .. method:: get_pools(self)



   
   .. method:: create_pool(self, name, slots, description)



   
   .. method:: delete_pool(self, name)



   
   .. method:: get_lineage(self, dag_id, execution_date)




