Support DAG versioning by introducing DAG Bundles

The following DAG parsing configuration options were moved into the ``dag_processor`` section:

* ``[core] dag_file_processor_timeout`` → ``[dag_processor] dag_file_processor_timeout``
* ``[scheduler] parsing_processes`` → ``[dag_processor] parsing_processes``
* ``[scheduler] file_parsing_sort_mode`` → ``[dag_processor] file_parsing_sort_mode``
* ``[scheduler] max_callbacks_per_loop`` → ``[dag_processor] max_callbacks_per_loop``
* ``[scheduler] min_file_process_interval`` → ``[dag_processor] min_file_process_interval``
* ``[scheduler] stale_dag_threshold`` → ``[dag_processor] stale_dag_threshold``
* ``[scheduler] print_stats_interval`` → ``[dag_processor] print_stats_interval``

The following DAG parsing configuration options were moved into the ``logging`` section:

* ``[scheduler] child_process_log_directory`` → ``[logging] dag_processor_child_process_log_directory``

The default value of ``[logging] dag_processor_child_process_log_directory`` was changed from
``AIRFLOW_HOME/logs/scheduler`` to ``AIRFLOW_HOME/logs/dag-processor``, which moves the parsing logs for dag files into
that new location.

The "subdir" concept has been superseded by the "bundle" concept. Users are able to
define separate bundles for different DAG folders, and can refer to them by the bundle name
instead of their location on disk.

The ``-subdir`` option of the following commands has been replaced with ``--bundle-name``:

* ``airflow dag-processor``
* ``airflow dags list-import-errors``
* ``airflow dags report``

The ``--subdir`` option has been removed from the following commands:

* ``airflow dags next-execution``
* ``airflow dags pause``
* ``airflow dags show``
* ``airflow dags show-dependencies``
* ``airflow dags state``
* ``airflow dags test``
* ``airflow dags trigger``
* ``airflow dags unpause``

Dag bundles are not initialized in the triggerer. In practice, this means that triggers cannot come from a dag bundle.
This is because the triggerer does not deal with changes in trigger code over time, as everything happens in the main process.
Triggers can come from anywhere else on ``sys.path`` instead.

.. Provide additional contextual information

.. Check the type of change that applies to this change

* Types of change

  * [ ] Dag changes
  * [x] Config changes
  * [ ] API changes
  * [x] CLI changes
  * [ ] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [ ] Code interface changes

.. List the migration rules needed for this change (see https://github.com/apache/airflow/issues/41641)

* Migration rules needed

  * ``airflow config lint``

    * [x] ``[core] dag_file_processor_timeout`` → ``[dag_processor] dag_file_processor_timeout``
    * [x] ``[scheduler] parsing_processes`` → ``[dag_processor] parsing_processes``
    * [x] ``[scheduler] file_parsing_sort_mode`` → ``[dag_processor] file_parsing_sort_mode``
    * [x] ``[scheduler] max_callbacks_per_loop`` → ``[dag_processor] max_callbacks_per_loop``
    * [x] ``[scheduler] min_file_process_interval`` → ``[dag_processor] min_file_process_interval``
    * [x] ``[scheduler] stale_dag_threshold`` → ``[dag_processor] stale_dag_threshold``
    * [x] ``[scheduler] print_stats_interval`` → ``[dag_processor] print_stats_interval``
    * [x] ``[scheduler] dag_dir_list_interval`` → ``[dag_processor] refresh_interval``
    * [x] ``[scheduler] dag_dir_list_interval`` → ``[logging] dag_processor_child_process_log_directory``
