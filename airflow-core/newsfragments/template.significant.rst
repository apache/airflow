.. Write a short and imperative summary of this changes

.. Provide additional contextual information

.. Check the type of change that applies to this change
.. Dag changes: requires users to change their Dag code
.. Config changes: requires users to change their Airflow config
.. API changes: requires users to change their Airflow REST API calls
.. CLI changes: requires users to change their Airflow CLI usage
.. Behaviour changes: the existing code won't break, but the behavior is different
.. Plugin changes: requires users to change their Airflow plugin implementation
.. Dependency changes: requires users to change their dependencies (e.g., Postgres 12)
.. Code interface changes: requires users to change other implementations (e.g., auth manager)

* Types of change

  * [ ] Dag changes
  * [ ] Config changes
  * [ ] API changes
  * [ ] CLI changes
  * [ ] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [ ] Code interface changes

.. List the migration rules needed for this change (see https://github.com/apache/airflow/issues/41641)

* Migration rules needed

.. e.g.,
.. * Remove context key ``execution_date``
.. * context key ``triggering_dataset_events`` → ``triggering_asset_events``
.. * Remove method ``airflow.providers_manager.ProvidersManager.initialize_providers_dataset_uri_resources`` → ``airflow.providers_manager.ProvidersManager.initialize_providers_asset_uri_resources``
