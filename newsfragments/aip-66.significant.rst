Support DAG versioning by introducing DAG Bundles

The following DAG parsing configuration options were moved into the ``dag_processor`` section:

* ``[core] dag_file_processor_timeout`` → ``[dag_processor] dag_file_processor_timeout``
* ``[scheduler] parsing_processes`` → ``[dag_processor] parsing_processes``
* ``[scheduler] file_parsing_sort_mode`` → ``[dag_processor] file_parsing_sort_mode``
* ``[scheduler] max_callbacks_per_loop`` → ``[dag_processor] max_callbacks_per_loop``
* ``[scheduler] min_file_process_interval`` → ``[dag_processor] min_file_process_interval``
* ``[scheduler] stale_dag_threshold`` → ``[dag_processor] stale_dag_threshold``
* ``[scheduler] print_stats_interval`` → ``[dag_processor] print_stats_interval``

The "subdir" concept has been superseded by the "bundle" concept. Users are able to
define separate bundles for different DAG folders, and can refer to them by the bundle name
instead of their location on disk.

The ``-subdir`` option of the following commands has been replaced with ``--bundle-name``:

* ``airflow dag-processor``

The ``--subdir`` option has been removed from the following commands (it was a noop):

* ``airflow dags pause``
* ``airflow dags unpause``

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
