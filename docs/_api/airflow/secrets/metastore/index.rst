:mod:`airflow.secrets.metastore`
================================

.. py:module:: airflow.secrets.metastore

.. autoapi-nested-parse::

   Objects relating to sourcing connections from metastore database



Module Contents
---------------

.. py:class:: MetastoreBackend

   Bases: :class:`airflow.secrets.BaseSecretsBackend`

   Retrieves Connection object from airflow metastore database.

   
   .. method:: get_connections(self, conn_id, session=None)



   
   .. method:: get_variable(self, key: str, session=None)

      Get Airflow Variable from Metadata DB

      :param key: Variable Key
      :return: Variable Value




