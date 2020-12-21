:mod:`airflow.api.client`
=========================

.. py:module:: airflow.api.client

.. autoapi-nested-parse::

   API Client that allows interacting with Airflow API



Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   api_client/index.rst
   json_client/index.rst
   local_client/index.rst


Package Contents
----------------

.. py:class:: Client(api_base_url, auth=None, session=None)

   Base API client for all API clients.

   
   .. method:: trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None)

      Create a dag run for the specified dag.

      :param dag_id:
      :param run_id:
      :param conf:
      :param execution_date:
      :return:



   
   .. method:: delete_dag(self, dag_id)

      Delete all DB records related to the specified dag.

      :param dag_id:



   
   .. method:: get_pool(self, name)

      Get pool.

      :param name: pool name



   
   .. method:: get_pools(self)

      Get all pools.



   
   .. method:: create_pool(self, name, slots, description)

      Create a pool.

      :param name: pool name
      :param slots: pool slots amount
      :param description: pool description



   
   .. method:: delete_pool(self, name)

      Delete pool.

      :param name: pool name



   
   .. method:: get_lineage(self, dag_id: str, execution_date: str)

      Return the lineage information for the dag on this execution date
      :param dag_id:
      :param execution_date:
      :return:




.. data:: conf
   

   

.. function:: get_current_api_client() -> Client
   Return current API Client based on current Airflow configuration


