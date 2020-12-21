:mod:`airflow.models.dagbag`
============================

.. py:module:: airflow.models.dagbag


Module Contents
---------------

.. py:class:: FileLoadStat

   Bases: :class:`typing.NamedTuple`

   Information about single file

   .. attribute:: file
      :annotation: :str

      

   .. attribute:: duration
      :annotation: :timedelta

      

   .. attribute:: dag_num
      :annotation: :int

      

   .. attribute:: task_num
      :annotation: :int

      

   .. attribute:: dags
      :annotation: :str

      


.. py:class:: DagBag(dag_folder: Optional[str] = None, include_examples: bool = conf.getboolean('core', 'LOAD_EXAMPLES'), include_smart_sensor: bool = conf.getboolean('smart_sensor', 'USE_SMART_SENSOR'), safe_mode: bool = conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'), read_dags_from_db: bool = False, store_serialized_dags: Optional[bool] = None)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   A dagbag is a collection of dags, parsed out of a folder tree and has high
   level configuration settings, like what database to use as a backend and
   what executor to use to fire off tasks. This makes it easier to run
   distinct environments for say production and development, tests, or for
   different teams or security profiles. What would have been system level
   settings are now dagbag level so that one system can run multiple,
   independent settings sets.

   :param dag_folder: the folder to scan to find DAGs
   :type dag_folder: unicode
   :param include_examples: whether to include the examples that ship
       with airflow or not
   :type include_examples: bool
   :param include_smart_sensor: whether to include the smart sensor native
       DAGs that create the smart sensor operators for whole cluster
   :type include_smart_sensor: bool
   :param read_dags_from_db: Read DAGs from DB if ``True`` is passed.
       If ``False`` DAGs are read from python files.
   :type read_dags_from_db: bool

   .. attribute:: DAGBAG_IMPORT_TIMEOUT
      

      

   .. attribute:: SCHEDULER_ZOMBIE_TASK_THRESHOLD
      

      

   .. attribute:: store_serialized_dags
      

      Whether or not to read dags from DB


   .. attribute:: dag_ids
      

      :return: a list of DAG IDs in this bag
      :rtype: List[unicode]


   
   .. method:: size(self)

      :return: the amount of dags contained in this dagbag



   
   .. method:: get_dag(self, dag_id, session: Session = None)

      Gets the DAG out of the dictionary, and refreshes it if expired

      :param dag_id: DAG Id
      :type dag_id: str



   
   .. method:: _add_dag_from_db(self, dag_id: str, session: Session)

      Add DAG to DagBag from DB



   
   .. method:: process_file(self, filepath, only_if_updated=True, safe_mode=True)

      Given a path to a python module or zip file, this method imports
      the module and look for dag objects within it.



   
   .. method:: _load_modules_from_file(self, filepath, safe_mode)



   
   .. method:: _load_modules_from_zip(self, filepath, safe_mode)



   
   .. method:: _process_modules(self, filepath, mods, file_last_changed_on_disk)



   
   .. method:: bag_dag(self, dag, root_dag)

      Adds the DAG into the bag, recurses into sub dags.
      Throws AirflowDagCycleException if a cycle is detected in this dag or its subdags



   
   .. method:: collect_dags(self, dag_folder=None, only_if_updated=True, include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'), include_smart_sensor=conf.getboolean('smart_sensor', 'USE_SMART_SENSOR'), safe_mode=conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'))

      Given a file path or a folder, this method looks for python modules,
      imports them and adds them to the dagbag collection.

      Note that if a ``.airflowignore`` file is found while processing
      the directory, it will behave much like a ``.gitignore``,
      ignoring files that match any of the regex patterns specified
      in the file.

      **Note**: The patterns in .airflowignore are treated as
      un-anchored regexes, not shell-like glob patterns.



   
   .. method:: collect_dags_from_db(self)

      Collects DAGs from database.



   
   .. method:: dagbag_report(self)

      Prints a report around DagBag loading stats



   
   .. method:: sync_to_db(self, session: Optional[Session] = None)

      Save attributes about list of DAG to the DB.




