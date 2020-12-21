:mod:`airflow.operators.sql`
============================

.. py:module:: airflow.operators.sql


Module Contents
---------------

.. data:: ALLOWED_CONN_TYPE
   

   

.. py:class:: SQLCheckOperator(*, sql: str, conn_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Performs checks against a db. The ``SQLCheckOperator`` expects
   a sql query that will return a single row. Each value on that
   first row is evaluated using python ``bool`` casting. If any of the
   values return ``False`` the check is failed and errors out.

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

   :param sql: the sql to be executed. (templated)
   :type sql: str

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['sql']

      

   .. attribute:: template_ext
      :annotation: :Iterable[str] = ['.hql', '.sql']

      

   .. attribute:: ui_color
      :annotation: = #fff7e6

      

   
   .. method:: execute(self, context=None)



   
   .. method:: get_db_hook(self)

      Get the database hook for the connection.

      :return: the database hook object.
      :rtype: DbApiHook




.. function:: _convert_to_float_if_possible(s)
   A small helper function to convert a string to a numeric value
   if appropriate

   :param s: the string to be converted
   :type s: str


.. py:class:: SQLValueCheckOperator(*, sql: str, pass_value: Any, tolerance: Any = None, conn_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Performs a simple value check using sql code.

   Note that this is an abstract class and get_db_hook
   needs to be defined. Whereas a get_db_hook is hook that gets a
   single record from an external source.

   :param sql: the sql to be executed. (templated)
   :type sql: str

   .. attribute:: __mapper_args__
      

      

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['sql', 'pass_value']

      

   .. attribute:: template_ext
      :annotation: :Iterable[str] = ['.hql', '.sql']

      

   .. attribute:: ui_color
      :annotation: = #fff7e6

      

   
   .. method:: execute(self, context=None)



   
   .. method:: _to_float(self, records)



   
   .. method:: _get_string_matches(self, records, pass_value_conv)



   
   .. method:: _get_numeric_matches(self, numeric_records, numeric_pass_value_conv)



   
   .. method:: get_db_hook(self)

      Get the database hook for the connection.

      :return: the database hook object.
      :rtype: DbApiHook




.. py:class:: SQLIntervalCheckOperator(*, table: str, metrics_thresholds: Dict[str, int], date_filter_column: Optional[str] = 'ds', days_back: SupportsAbs[int] = -7, ratio_formula: Optional[str] = 'max_over_min', ignore_zero: bool = True, conn_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Checks that the values of metrics given as SQL expressions are within
   a certain tolerance of the ones from days_back before.

   Note that this is an abstract class and get_db_hook
   needs to be defined. Whereas a get_db_hook is hook that gets a
   single record from an external source.

   :param table: the table name
   :type table: str
   :param days_back: number of days between ds and the ds we want to check
       against. Defaults to 7 days
   :type days_back: int
   :param ratio_formula: which formula to use to compute the ratio between
       the two metrics. Assuming cur is the metric of today and ref is
       the metric to today - days_back.

       max_over_min: computes max(cur, ref) / min(cur, ref)
       relative_diff: computes abs(cur-ref) / ref

       Default: 'max_over_min'
   :type ratio_formula: str
   :param ignore_zero: whether we should ignore zero metrics
   :type ignore_zero: bool
   :param metrics_threshold: a dictionary of ratios indexed by metrics
   :type metrics_threshold: dict

   .. attribute:: __mapper_args__
      

      

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['sql1', 'sql2']

      

   .. attribute:: template_ext
      :annotation: :Iterable[str] = ['.hql', '.sql']

      

   .. attribute:: ui_color
      :annotation: = #fff7e6

      

   .. attribute:: ratio_formulas
      

      

   
   .. method:: execute(self, context=None)



   
   .. method:: get_db_hook(self)

      Get the database hook for the connection.

      :return: the database hook object.
      :rtype: DbApiHook




.. py:class:: SQLThresholdCheckOperator(*, sql: str, min_threshold: Any, max_threshold: Any, conn_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Performs a value check using sql code against a minimum threshold
   and a maximum threshold. Thresholds can be in the form of a numeric
   value OR a sql statement that results a numeric.

   Note that this is an abstract class and get_db_hook
   needs to be defined. Whereas a get_db_hook is hook that gets a
   single record from an external source.

   :param sql: the sql to be executed. (templated)
   :type sql: str
   :param min_threshold: numerical value or min threshold sql to be executed (templated)
   :type min_threshold: numeric or str
   :param max_threshold: numerical value or max threshold sql to be executed (templated)
   :type max_threshold: numeric or str

   .. attribute:: template_fields
      :annotation: = ['sql', 'min_threshold', 'max_threshold']

      

   .. attribute:: template_ext
      :annotation: :Iterable[str] = ['.hql', '.sql']

      

   
   .. method:: execute(self, context=None)



   
   .. method:: push(self, meta_data)

      Optional: Send data check info and metadata to an external database.
      Default functionality will log metadata.



   
   .. method:: get_db_hook(self)

      Returns DB hook




.. py:class:: BranchSQLOperator(*, sql: str, follow_task_ids_if_true: List[str], follow_task_ids_if_false: List[str], conn_id: str = 'default_conn_id', database: Optional[str] = None, parameters: Optional[Union[Mapping, Iterable]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.models.SkipMixin`

   Executes sql code in a specific database

   :param sql: the sql code to be executed. (templated)
   :type sql: Can receive a str representing a sql statement or reference to a template file.
              Template reference are recognized by str ending in '.sql'.
              Expected SQL query to return Boolean (True/False), integer (0 = False, Otherwise = 1)
              or string (true/y/yes/1/on/false/n/no/0/off).
   :param follow_task_ids_if_true: task id or task ids to follow if query return true
   :type follow_task_ids_if_true: str or list
   :param follow_task_ids_if_false: task id or task ids to follow if query return true
   :type follow_task_ids_if_false: str or list
   :param conn_id: reference to a specific database
   :type conn_id: str
   :param database: name of database which overwrite defined one in connection
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: mapping or iterable

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #a22034

      

   .. attribute:: ui_fgcolor
      :annotation: = #F7F7F7

      

   
   .. method:: _get_hook(self)



   
   .. method:: execute(self, context: Dict)




