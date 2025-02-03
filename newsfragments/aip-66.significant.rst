Support DAG versioning by introducing DAG Bundles

The following DAG parsing configuration options were moved into the ``dag_processor`` section:

* ``[core] dag_file_processor_timeout`` -> ``[dag_processor] dag_file_processor_timeout``
* ``[scheduler] parsing_processes`` -> ``[dag_processor] parsing_processes``
* ``[scheduler] file_parsing_sort_mode`` -> ``[dag_processor] file_parsing_sort_mode``
* ``[scheduler] max_callbacks_per_loop`` -> ``[dag_processor] max_callbacks_per_loop``
* ``[scheduler] min_file_process_interval`` -> ``[dag_processor] min_file_process_interval``
* ``[scheduler] stale_dag_threshold`` -> ``[dag_processor] stale_dag_threshold``
* ``[scheduler] print_stats_interval`` -> ``[dag_processor] print_stats_interval``

.. Provide additional contextual information

.. Check the type of change that applies to this change

* Types of change

  * [ ] DAG changes
  * [x] Config changes
  * [ ] API changes
  * [ ] CLI changes
  * [ ] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency change

.. List the migration rules needed for this change (see https://github.com/apache/airflow/issues/41641)

* [ ] Migration rules needed

  * ``[core] dag_file_processor_timeout`` -> ``[dag_processor] dag_file_processor_timeout``
  * ``[scheduler] parsing_processes`` -> ``[dag_processor] parsing_processes``
  * ``[scheduler] file_parsing_sort_mode`` -> ``[dag_processor] file_parsing_sort_mode``
  * ``[scheduler] max_callbacks_per_loop`` -> ``[dag_processor] max_callbacks_per_loop``
  * ``[scheduler] min_file_process_interval`` -> ``[dag_processor] min_file_process_interval``
  * ``[scheduler] stale_dag_threshold`` -> ``[dag_processor] stale_dag_threshold``
  * ``[scheduler] print_stats_interval`` -> ``[dag_processor] print_stats_interval``
  * ``[scheduler] dag_dir_list_interval`` → ``[dag_processor] refresh_interval``
