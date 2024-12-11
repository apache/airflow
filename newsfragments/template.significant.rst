.. Write a short and imperative summary of this changes

.. Provide additional contextual information

.. Check the type of change that applies to this change

* Types of change

  * [ ] DAG changes
  * [ ] Config changes
  * [ ] API changes
  * [ ] CLI changes
  * [ ] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency change

.. List the migration rules needed for this change (see https://github.com/apache/airflow/issues/41641)

* Migration rules needed

.. e.g.,
.. * Remove context key ``execution_date``
.. * context key ``triggering_dataset_events`` → ``triggering_asset_events``
.. * Remove method ``airflow.providers_manager.ProvidersManager.initialize_providers_dataset_uri_resources`` → ``airflow.providers_manager.ProvidersManager.initialize_providers_asset_uri_resources``
