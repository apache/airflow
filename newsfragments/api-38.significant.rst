- With the new UI and we have removed DAG and config level settings that control some of the the UI behaviour.

  These are now as per-user settings controlled by the UI

  - ``default_view``
  - ``orientation``

* Types of change

  * [x] Dag changes
  * [ ] Config changes
  * [ ] API changes
  * [ ] CLI changes
  * [ ] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [ ] Code interface changes

* Migration rules needed

  * ruff

    * ``airflow config lint``

      * [ ] ``core.dag_default_view``
      * [ ] ``core.dag_orientation``

    * AIR302

      * [ ] ``default_view`` argument to DAG removed
      * [ ] ``orientation`` argument to DAG removed
