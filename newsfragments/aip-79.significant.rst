Remove Flask App Builder from core Airflow dependencies.

As part of this change the following breaking changes have occurred:

- The auth manager interface ``base_auth_manager`` have been updated with some breaking changes:

  - The constructor no longer take ``appbuilder`` as parameter. The constructor takes no parameter

  - A new abstract method ``deserialize_user`` needs to be implemented

  - A new abstract method ``serialize_user`` needs to be implemented

  - The property ``security_manager`` has been removed from the interface

  - The method ``filter_permitted_menu_items`` is now abstract and must be implemented
