:mod:`airflow.providers.qubole.hooks.qubole_check`
==================================================

.. py:module:: airflow.providers.qubole.hooks.qubole_check


Module Contents
---------------

.. data:: log
   

   

.. data:: COL_DELIM
   :annotation: = 	

   

.. data:: ROW_DELIM
   :annotation: = 


   

.. function:: isint(value) -> bool
   Whether Qubole column are integer


.. function:: isfloat(value) -> bool
   Whether Qubole column are float


.. function:: isbool(value) -> bool
   Whether Qubole column are boolean


.. function:: parse_first_row(row_list) -> List[Union[bool, float, int, str]]
   Parse Qubole first record list


.. py:class:: QuboleCheckHook(context, *args, **kwargs)

   Bases: :class:`airflow.providers.qubole.hooks.qubole.QuboleHook`

   Qubole check hook

   
   .. staticmethod:: handle_failure_retry(context)



   
   .. method:: get_first(self, sql)

      Get Qubole query first record list



   
   .. method:: get_query_results(self)

      Get Qubole query result




