:mod:`airflow.providers.apache.druid.operators.druid`
=====================================================

.. py:module:: airflow.providers.apache.druid.operators.druid


Module Contents
---------------

.. py:class:: DruidOperator(*, json_index_file: str, druid_ingest_conn_id: str = 'druid_ingest_default', max_ingestion_time: Optional[int] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Allows to submit a task directly to druid

   :param json_index_file: The filepath to the druid index specification
   :type json_index_file: str
   :param druid_ingest_conn_id: The connection id of the Druid overlord which
       accepts index jobs
   :type druid_ingest_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['json_index_file']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   
   .. method:: execute(self, context: Dict[Any, Any])




