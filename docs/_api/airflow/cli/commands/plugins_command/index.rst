:mod:`airflow.cli.commands.plugins_command`
===========================================

.. py:module:: airflow.cli.commands.plugins_command


Module Contents
---------------

.. data:: PLUGINS_MANAGER_ATTRIBUTES_TO_DUMP
   :annotation: = ['plugins', 'import_errors', 'macros_modules', 'executors_modules', 'flask_blueprints', 'flask_appbuilder_views', 'flask_appbuilder_menu_links', 'global_operator_extra_links', 'operator_extra_links', 'registered_operator_link_classes']

   

.. data:: PLUGINS_ATTRIBUTES_TO_DUMP
   :annotation: = ['hooks', 'executors', 'macros', 'flask_blueprints', 'appbuilder_views', 'appbuilder_menu_items', 'global_operator_extra_links', 'operator_extra_links', 'source']

   

.. function:: _header(text, fillchar)

.. function:: dump_plugins(args)
   Dump plugins information


