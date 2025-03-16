Remove Flask App Builder from core Airflow dependencies.

As part of this change the following breaking changes have occurred:

- The auth manager interface ``base_auth_manager`` have been updated with some breaking changes:

  - The constructor no longer take ``appbuilder`` as parameter. The constructor takes no parameter

  - A new abstract method ``deserialize_user`` needs to be implemented

  - A new abstract method ``serialize_user`` needs to be implemented

  - A new abstract method ``filter_authorized_menu_items`` needs to be implemented

  - The property ``security_manager`` has been removed from the interface

  - The method ``get_url_logout`` is now optional

  - The method ``get_permitted_dag_ids`` has been renamed ``get_authorized_dag_ids``

  - The method ``filter_permitted_dag_ids`` has been renamed ``filter_authorized_dag_ids``

  - All these methods have been removed from the interface:

    - ``filter_permitted_menu_items``
    - ``get_user_name``
    - ``get_user_display_name``
    - ``get_user``
    - ``get_user_id``
    - ``is_logged_in``
    - ``get_api_endpoints``
    - ``register_views``

  - All the following method signatures changed to make the parameter ``user`` required (it was optional)

    - ``is_authorized_configuration``
    - ``is_authorized_connection``
    - ``is_authorized_dag``
    - ``is_authorized_asset``
    - ``is_authorized_pool``
    - ``is_authorized_variable``
    - ``is_authorized_view``
    - ``is_authorized_custom_view``
    - ``get_authorized_dag_ids`` (previously ``get_permitted_dag_ids``)
    - ``filter_authorized_dag_ids`` (previously ``filter_permitted_dag_ids``)

  - All the following method signatures changed to add the parameter ``user``

    - ``batch_is_authorized_connection``
    - ``batch_is_authorized_dag``
    - ``batch_is_authorized_pool``
    - ``batch_is_authorized_variable``

- The module ``airflow.www.auth`` has been moved to ``airflow.providers.fab.www.auth``

* Types of change

  * [ ] Dag changes
  * [ ] Config changes
  * [ ] API changes
  * [ ] CLI changes
  * [ ] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [x] Code interface changes
