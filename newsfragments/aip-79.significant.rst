Remove Flask App Builder from core Airflow dependencies.

As part of this change the following breaking changes have occurred:

- The auth manager interface ``base_auth_manager`` have been updated with some breaking changes:

  - The constructor no longer take ``appbuilder`` as parameter. The constructor takes no parameter

  - A new abstract method ``deserialize_user`` needs to be implemented

  - A new abstract method ``serialize_user`` needs to be implemented

  - The property ``security_manager`` has been removed from the interface

  - The method ``filter_permitted_menu_items`` is now abstract and must be implemented

  - All the following method signatures changed to make the parameter ``user`` required (it was optional)

    - ``is_authorized_configuration``
    - ``is_authorized_connection``
    - ``is_authorized_dag``
    - ``is_authorized_asset``
    - ``is_authorized_pool``
    - ``is_authorized_variable``
    - ``is_authorized_view``
    - ``is_authorized_custom_view``
    - ``get_permitted_dag_ids``
    - ``filter_permitted_dag_ids``

  - All the following method signatures changed to add the parameter ``user``

    - ``batch_is_authorized_connection``
    - ``batch_is_authorized_dag``
    - ``batch_is_authorized_pool``
    - ``batch_is_authorized_variable``

* Types of change

  * [ ] Dag changes
  * [ ] Config changes
  * [ ] API changes
  * [ ] CLI changes
  * [ ] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [x] Code interface changes
