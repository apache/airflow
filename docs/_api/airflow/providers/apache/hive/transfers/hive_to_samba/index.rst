:mod:`airflow.providers.apache.hive.transfers.hive_to_samba`
============================================================

.. py:module:: airflow.providers.apache.hive.transfers.hive_to_samba

.. autoapi-nested-parse::

   This module contains operator to move data from Hive to Samba.



Module Contents
---------------

.. py:class:: HiveToSambaOperator(*, hql: str, destination_filepath: str, samba_conn_id: str = 'samba_default', hiveserver2_conn_id: str = 'hiveserver2_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes hql code in a specific Hive database and loads the
   results of the query as a csv to a Samba location.

   :param hql: the hql to be exported. (templated)
   :type hql: str
   :param destination_filepath: the file path to where the file will be pushed onto samba
   :type destination_filepath: str
   :param samba_conn_id: reference to the samba destination
   :type samba_conn_id: str
   :param hiveserver2_conn_id: reference to the hiveserver2 service
   :type hiveserver2_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['hql', 'destination_filepath']

      

   .. attribute:: template_ext
      :annotation: = ['.hql', '.sql']

      

   
   .. method:: execute(self, context)




