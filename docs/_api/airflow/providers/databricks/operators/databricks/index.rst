:mod:`airflow.providers.databricks.operators.databricks`
========================================================

.. py:module:: airflow.providers.databricks.operators.databricks

.. autoapi-nested-parse::

   This module contains Databricks operators.



Module Contents
---------------

.. data:: XCOM_RUN_ID_KEY
   :annotation: = run_id

   

.. data:: XCOM_RUN_PAGE_URL_KEY
   :annotation: = run_page_url

   

.. function:: _deep_string_coerce(content, json_path: str = 'json') -> Union[str, list, dict]
   Coerces content or all values of content if it is a dict to a string. The
   function will throw if content contains non-string or non-numeric types.

   The reason why we have this function is because the ``self.json`` field must be a
   dict with only string values. This is because ``render_template`` will fail
   for numerical values.


.. function:: _handle_databricks_operator_execution(operator, hook, log, context) -> None
   Handles the Airflow + Databricks lifecycle logic for a Databricks operator

   :param operator: Databricks operator being handled
   :param context: Airflow context


.. py:class:: DatabricksSubmitRunOperator(*, json: Optional[Any] = None, spark_jar_task: Optional[Dict[str, str]] = None, notebook_task: Optional[Dict[str, str]] = None, spark_python_task: Optional[Dict[str, Union[str, List[str]]]] = None, spark_submit_task: Optional[Dict[str, List[str]]] = None, new_cluster: Optional[Dict[str, object]] = None, existing_cluster_id: Optional[str] = None, libraries: Optional[List[Dict[str, str]]] = None, run_name: Optional[str] = None, timeout_seconds: Optional[int] = None, databricks_conn_id: str = 'databricks_default', polling_period_seconds: int = 30, databricks_retry_limit: int = 3, databricks_retry_delay: int = 1, do_xcom_push: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Submits a Spark job run to Databricks using the
   `api/2.0/jobs/runs/submit
   <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_
   API endpoint.

   There are two ways to instantiate this operator.

   In the first way, you can take the JSON payload that you typically use
   to call the ``api/2.0/jobs/runs/submit`` endpoint and pass it directly
   to our ``DatabricksSubmitRunOperator`` through the ``json`` parameter.
   For example ::

       json = {
         'new_cluster': {
           'spark_version': '2.1.0-db3-scala2.11',
           'num_workers': 2
         },
         'notebook_task': {
           'notebook_path': '/Users/airflow@example.com/PrepareData',
         },
       }
       notebook_run = DatabricksSubmitRunOperator(task_id='notebook_run', json=json)

   Another way to accomplish the same thing is to use the named parameters
   of the ``DatabricksSubmitRunOperator`` directly. Note that there is exactly
   one named parameter for each top level parameter in the ``runs/submit``
   endpoint. In this method, your code would look like this: ::

       new_cluster = {
         'spark_version': '2.1.0-db3-scala2.11',
         'num_workers': 2
       }
       notebook_task = {
         'notebook_path': '/Users/airflow@example.com/PrepareData',
       }
       notebook_run = DatabricksSubmitRunOperator(
           task_id='notebook_run',
           new_cluster=new_cluster,
           notebook_task=notebook_task)

   In the case where both the json parameter **AND** the named parameters
   are provided, they will be merged together. If there are conflicts during the merge,
   the named parameters will take precedence and override the top level ``json`` keys.

   Currently the named parameters that ``DatabricksSubmitRunOperator`` supports are
       - ``spark_jar_task``
       - ``notebook_task``
       - ``spark_python_task``
       - ``spark_submit_task``
       - ``new_cluster``
       - ``existing_cluster_id``
       - ``libraries``
       - ``run_name``
       - ``timeout_seconds``

   :param json: A JSON object containing API parameters which will be passed
       directly to the ``api/2.0/jobs/runs/submit`` endpoint. The other named parameters
       (i.e. ``spark_jar_task``, ``notebook_task``..) to this operator will
       be merged with this json dictionary if they are provided.
       If there are conflicts during the merge, the named parameters will
       take precedence and override the top level json keys. (templated)

       .. seealso::
           For more information about templating see :ref:`jinja-templating`.
           https://docs.databricks.com/api/latest/jobs.html#runs-submit
   :type json: dict
   :param spark_jar_task: The main class and parameters for the JAR task. Note that
       the actual JAR is specified in the ``libraries``.
       *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
       *OR* ``spark_submit_task`` should be specified.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/api/latest/jobs.html#jobssparkjartask
   :type spark_jar_task: dict
   :param notebook_task: The notebook path and parameters for the notebook task.
       *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
       *OR* ``spark_submit_task`` should be specified.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/api/latest/jobs.html#jobsnotebooktask
   :type notebook_task: dict
   :param spark_python_task: The python file path and parameters to run the python file with.
       *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
       *OR* ``spark_submit_task`` should be specified.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/api/latest/jobs.html#jobssparkpythontask
   :type spark_python_task: dict
   :param spark_submit_task: Parameters needed to run a spark-submit command.
       *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
       *OR* ``spark_submit_task`` should be specified.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/api/latest/jobs.html#jobssparksubmittask
   :type spark_submit_task: dict
   :param new_cluster: Specs for a new cluster on which this task will be run.
       *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/api/latest/jobs.html#jobsclusterspecnewcluster
   :type new_cluster: dict
   :param existing_cluster_id: ID for existing cluster on which to run this task.
       *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified.
       This field will be templated.
   :type existing_cluster_id: str
   :param libraries: Libraries which this run will use.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/api/latest/libraries.html#managedlibrarieslibrary
   :type libraries: list of dicts
   :param run_name: The run name used for this task.
       By default this will be set to the Airflow ``task_id``. This ``task_id`` is a
       required parameter of the superclass ``BaseOperator``.
       This field will be templated.
   :type run_name: str
   :param timeout_seconds: The timeout for this run. By default a value of 0 is used
       which means to have no timeout.
       This field will be templated.
   :type timeout_seconds: int32
   :param databricks_conn_id: The name of the Airflow connection to use.
       By default and in the common case this will be ``databricks_default``. To use
       token based authentication, provide the key ``token`` in the extra field for the
       connection and create the key ``host`` and leave the ``host`` field empty.
   :type databricks_conn_id: str
   :param polling_period_seconds: Controls the rate which we poll for the result of
       this run. By default the operator will poll every 30 seconds.
   :type polling_period_seconds: int
   :param databricks_retry_limit: Amount of times retry if the Databricks backend is
       unreachable. Its value must be greater than or equal to 1.
   :type databricks_retry_limit: int
   :param databricks_retry_delay: Number of seconds to wait between retries (it
           might be a floating point number).
   :type databricks_retry_delay: float
   :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
   :type do_xcom_push: bool

   .. attribute:: template_fields
      :annotation: = ['json']

      

   .. attribute:: ui_color
      :annotation: = #1CB1C2

      

   .. attribute:: ui_fgcolor
      :annotation: = #fff

      

   
   .. method:: _get_hook(self)



   
   .. method:: execute(self, context)



   
   .. method:: on_kill(self)




.. py:class:: DatabricksRunNowOperator(*, job_id: Optional[str] = None, json: Optional[Any] = None, notebook_params: Optional[Dict[str, str]] = None, python_params: Optional[List[str]] = None, spark_submit_params: Optional[List[str]] = None, databricks_conn_id: str = 'databricks_default', polling_period_seconds: int = 30, databricks_retry_limit: int = 3, databricks_retry_delay: int = 1, do_xcom_push: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Runs an existing Spark job run to Databricks using the
   `api/2.0/jobs/run-now
   <https://docs.databricks.com/api/latest/jobs.html#run-now>`_
   API endpoint.

   There are two ways to instantiate this operator.

   In the first way, you can take the JSON payload that you typically use
   to call the ``api/2.0/jobs/run-now`` endpoint and pass it directly
   to our ``DatabricksRunNowOperator`` through the ``json`` parameter.
   For example ::

       json = {
         "job_id": 42,
         "notebook_params": {
           "dry-run": "true",
           "oldest-time-to-consider": "1457570074236"
         }
       }

       notebook_run = DatabricksRunNowOperator(task_id='notebook_run', json=json)

   Another way to accomplish the same thing is to use the named parameters
   of the ``DatabricksRunNowOperator`` directly. Note that there is exactly
   one named parameter for each top level parameter in the ``run-now``
   endpoint. In this method, your code would look like this: ::

       job_id=42

       notebook_params = {
           "dry-run": "true",
           "oldest-time-to-consider": "1457570074236"
       }

       python_params = ["douglas adams", "42"]

       spark_submit_params = ["--class", "org.apache.spark.examples.SparkPi"]

       notebook_run = DatabricksRunNowOperator(
           job_id=job_id,
           notebook_params=notebook_params,
           python_params=python_params,
           spark_submit_params=spark_submit_params
       )

   In the case where both the json parameter **AND** the named parameters
   are provided, they will be merged together. If there are conflicts during the merge,
   the named parameters will take precedence and override the top level ``json`` keys.

   Currently the named parameters that ``DatabricksRunNowOperator`` supports are
       - ``job_id``
       - ``json``
       - ``notebook_params``
       - ``python_params``
       - ``spark_submit_params``


   :param job_id: the job_id of the existing Databricks job.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/api/latest/jobs.html#run-now
   :type job_id: str
   :param json: A JSON object containing API parameters which will be passed
       directly to the ``api/2.0/jobs/run-now`` endpoint. The other named parameters
       (i.e. ``notebook_params``, ``spark_submit_params``..) to this operator will
       be merged with this json dictionary if they are provided.
       If there are conflicts during the merge, the named parameters will
       take precedence and override the top level json keys. (templated)

       .. seealso::
           For more information about templating see :ref:`jinja-templating`.
           https://docs.databricks.com/api/latest/jobs.html#run-now
   :type json: dict
   :param notebook_params: A dict from keys to values for jobs with notebook task,
       e.g. "notebook_params": {"name": "john doe", "age":  "35"}.
       The map is passed to the notebook and will be accessible through the
       dbutils.widgets.get function. See Widgets for more information.
       If not specified upon run-now, the triggered run will use the
       job’s base parameters. notebook_params cannot be
       specified in conjunction with jar_params. The json representation
       of this field (i.e. {"notebook_params":{"name":"john doe","age":"35"}})
       cannot exceed 10,000 bytes.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/user-guide/notebooks/widgets.html
   :type notebook_params: dict
   :param python_params: A list of parameters for jobs with python tasks,
       e.g. "python_params": ["john doe", "35"].
       The parameters will be passed to python file as command line parameters.
       If specified upon run-now, it would overwrite the parameters specified in
       job setting.
       The json representation of this field (i.e. {"python_params":["john doe","35"]})
       cannot exceed 10,000 bytes.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/api/latest/jobs.html#run-now
   :type python_params: list[str]
   :param spark_submit_params: A list of parameters for jobs with spark submit task,
       e.g. "spark_submit_params": ["--class", "org.apache.spark.examples.SparkPi"].
       The parameters will be passed to spark-submit script as command line parameters.
       If specified upon run-now, it would overwrite the parameters specified
       in job setting.
       The json representation of this field cannot exceed 10,000 bytes.
       This field will be templated.

       .. seealso::
           https://docs.databricks.com/api/latest/jobs.html#run-now
   :type spark_submit_params: list[str]
   :param timeout_seconds: The timeout for this run. By default a value of 0 is used
       which means to have no timeout.
       This field will be templated.
   :type timeout_seconds: int32
   :param databricks_conn_id: The name of the Airflow connection to use.
       By default and in the common case this will be ``databricks_default``. To use
       token based authentication, provide the key ``token`` in the extra field for the
       connection and create the key ``host`` and leave the ``host`` field empty.
   :type databricks_conn_id: str
   :param polling_period_seconds: Controls the rate which we poll for the result of
       this run. By default the operator will poll every 30 seconds.
   :type polling_period_seconds: int
   :param databricks_retry_limit: Amount of times retry if the Databricks backend is
       unreachable. Its value must be greater than or equal to 1.
   :type databricks_retry_limit: int
   :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
   :type do_xcom_push: bool

   .. attribute:: template_fields
      :annotation: = ['json']

      

   .. attribute:: ui_color
      :annotation: = #1CB1C2

      

   .. attribute:: ui_fgcolor
      :annotation: = #fff

      

   
   .. method:: _get_hook(self)



   
   .. method:: execute(self, context)



   
   .. method:: on_kill(self)




