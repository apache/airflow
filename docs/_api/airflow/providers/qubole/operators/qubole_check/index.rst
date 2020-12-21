:mod:`airflow.providers.qubole.operators.qubole_check`
======================================================

.. py:module:: airflow.providers.qubole.operators.qubole_check


Module Contents
---------------

.. py:class:: QuboleCheckOperator(*, qubole_conn_id: str = 'qubole_default', **kwargs)

   Bases: :class:`airflow.operators.check_operator.CheckOperator`, :class:`airflow.providers.qubole.operators.qubole.QuboleOperator`

   Performs checks against Qubole Commands. ``QuboleCheckOperator`` expects
   a command that will be executed on QDS.
   By default, each value on first row of the result of this Qubole Command
   is evaluated using python ``bool`` casting. If any of the
   values return ``False``, the check is failed and errors out.

   Note that Python bool casting evals the following as ``False``:

   * ``False``
   * ``0``
   * Empty string (``""``)
   * Empty list (``[]``)
   * Empty dictionary or set (``{}``)

   Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
   the count ``== 0``. You can craft much more complex query that could,
   for instance, check that the table has the same number of rows as
   the source table upstream, or that the count of today's partition is
   greater than yesterday's partition, or that a set of metrics are less
   than 3 standard deviation for the 7 day average.

   This operator can be used as a data quality check in your pipeline, and
   depending on where you put it in your DAG, you have the choice to
   stop the critical path, preventing from
   publishing dubious data, or on the side and receive email alerts
   without stopping the progress of the DAG.

   :param qubole_conn_id: Connection id which consists of qds auth_token
   :type qubole_conn_id: str

   kwargs:

       Arguments specific to Qubole command can be referred from QuboleOperator docs.

       :results_parser_callable: This is an optional parameter to
           extend the flexibility of parsing the results of Qubole
           command to the users. This is a python callable which
           can hold the logic to parse list of rows returned by Qubole command.
           By default, only the values on first row are used for performing checks.
           This callable should return a list of records on
           which the checks have to be performed.

   .. note:: All fields in common with template fields of
       QuboleOperator and CheckOperator are template-supported.

   .. attribute:: template_fields
      :annotation: :Iterable[str]

      

   .. attribute:: template_ext
      

      

   .. attribute:: ui_fgcolor
      :annotation: = #000

      

   
   .. method:: execute(self, context=None)



   
   .. method:: get_db_hook(self)



   
   .. method:: get_hook(self, context=None)



   
   .. method:: __getattribute__(self, name: str)



   
   .. method:: __setattr__(self, name: str, value: str)




.. py:class:: QuboleValueCheckOperator(*, pass_value: Union[str, int, float], tolerance: Optional[Union[int, float]] = None, results_parser_callable=None, qubole_conn_id: str = 'qubole_default', **kwargs)

   Bases: :class:`airflow.operators.check_operator.ValueCheckOperator`, :class:`airflow.providers.qubole.operators.qubole.QuboleOperator`

   Performs a simple value check using Qubole command.
   By default, each value on the first row of this
   Qubole command is compared with a pre-defined value.
   The check fails and errors out if the output of the command
   is not within the permissible limit of expected value.

   :param qubole_conn_id: Connection id which consists of qds auth_token
   :type qubole_conn_id: str

   :param pass_value: Expected value of the query results.
   :type pass_value: str or int or float

   :param tolerance: Defines the permissible pass_value range, for example if
       tolerance is 2, the Qubole command output can be anything between
       -2*pass_value and 2*pass_value, without the operator erring out.

   :type tolerance: int or float


   kwargs:

       Arguments specific to Qubole command can be referred from QuboleOperator docs.

       :results_parser_callable: This is an optional parameter to
           extend the flexibility of parsing the results of Qubole
           command to the users. This is a python callable which
           can hold the logic to parse list of rows returned by Qubole command.
           By default, only the values on first row are used for performing checks.
           This callable should return a list of records on
           which the checks have to be performed.


   .. note:: All fields in common with template fields of
           QuboleOperator and ValueCheckOperator are template-supported.

   .. attribute:: template_fields
      

      

   .. attribute:: template_ext
      

      

   .. attribute:: ui_fgcolor
      :annotation: = #000

      

   
   .. method:: execute(self, context=None)



   
   .. method:: get_db_hook(self)



   
   .. method:: get_hook(self, context=None)



   
   .. method:: __getattribute__(self, name: str)



   
   .. method:: __setattr__(self, name: str, value: str)




.. function:: get_sql_from_qbol_cmd(params) -> str
   Get Qubole sql from Qubole command


.. function:: handle_airflow_exception(airflow_exception, hook)
   Qubole check handle Airflow exception


