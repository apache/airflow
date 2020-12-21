:mod:`airflow.plugins_manager`
==============================

.. py:module:: airflow.plugins_manager

.. autoapi-nested-parse::

   Manages all plugins.



Module Contents
---------------

.. data:: log
   

   

.. data:: import_errors
   :annotation: :Dict[str, str]

   

.. data:: plugins
   :annotation: :Optional[List[AirflowPlugin]]

   

.. data:: registered_hooks
   :annotation: :Optional[List['BaseHook']]

   

.. data:: macros_modules
   :annotation: :Optional[List[Any]]

   

.. data:: executors_modules
   :annotation: :Optional[List[Any]]

   

.. data:: admin_views
   :annotation: :Optional[List[Any]]

   

.. data:: flask_blueprints
   :annotation: :Optional[List[Any]]

   

.. data:: menu_links
   :annotation: :Optional[List[Any]]

   

.. data:: flask_appbuilder_views
   :annotation: :Optional[List[Any]]

   

.. data:: flask_appbuilder_menu_links
   :annotation: :Optional[List[Any]]

   

.. data:: global_operator_extra_links
   :annotation: :Optional[List[Any]]

   

.. data:: operator_extra_links
   :annotation: :Optional[List[Any]]

   

.. data:: registered_operator_link_classes
   :annotation: :Optional[Dict[str, Type]]

   Mapping of class names to class of OperatorLinks registered by plugins.

   Used by the DAG serialization code to only allow specific classes to be created
   during deserialization


.. py:class:: AirflowPluginSource

   Class used to define an AirflowPluginSource.

   
   .. method:: __str__(self)



   
   .. method:: __html__(self)




.. py:class:: PluginsDirectorySource(path)

   Bases: :class:`airflow.plugins_manager.AirflowPluginSource`

   Class used to define Plugins loaded from Plugins Directory.

   
   .. method:: __str__(self)



   
   .. method:: __html__(self)




.. py:class:: EntryPointSource(entrypoint)

   Bases: :class:`airflow.plugins_manager.AirflowPluginSource`

   Class used to define Plugins loaded from entrypoint.

   
   .. method:: __str__(self)



   
   .. method:: __html__(self)




.. py:exception:: AirflowPluginException

   Bases: :class:`Exception`

   Exception when loading plugin.


.. py:class:: AirflowPlugin

   Class used to define AirflowPlugin.

   .. attribute:: name
      :annotation: :Optional[str]

      

   .. attribute:: source
      :annotation: :Optional[AirflowPluginSource]

      

   .. attribute:: hooks
      :annotation: :List[Any] = []

      

   .. attribute:: executors
      :annotation: :List[Any] = []

      

   .. attribute:: macros
      :annotation: :List[Any] = []

      

   .. attribute:: admin_views
      :annotation: :List[Any] = []

      

   .. attribute:: flask_blueprints
      :annotation: :List[Any] = []

      

   .. attribute:: menu_links
      :annotation: :List[Any] = []

      

   .. attribute:: appbuilder_views
      :annotation: :List[Any] = []

      

   .. attribute:: appbuilder_menu_items
      :annotation: :List[Any] = []

      

   .. attribute:: global_operator_extra_links
      :annotation: :List[Any] = []

      

   .. attribute:: operator_extra_links
      :annotation: :List[Any] = []

      

   
   .. classmethod:: validate(cls)

      Validates that plugin has a name.



   
   .. classmethod:: on_load(cls, *args, **kwargs)

      Executed when the plugin is loaded.
      This method is only called once during runtime.

      :param args: If future arguments are passed in on call.
      :param kwargs: If future arguments are passed in on call.




.. function:: is_valid_plugin(plugin_obj)
   Check whether a potential object is a subclass of
   the AirflowPlugin class.

   :param plugin_obj: potential subclass of AirflowPlugin
   :return: Whether or not the obj is a valid subclass of
       AirflowPlugin


.. function:: load_entrypoint_plugins()
   Load and register plugins AirflowPlugin subclasses from the entrypoints.
   The entry_point group should be 'airflow.plugins'.


.. function:: load_plugins_from_plugin_directory()
   Load and register Airflow Plugins from plugins directory


.. function:: make_module(name: str, objects: List[Any])
   Creates new module.


.. function:: ensure_plugins_loaded()
   Load plugins from plugins directory and entrypoints.

   Plugins are only loaded if they have not been previously loaded.


.. function:: initialize_web_ui_plugins()
   Collect extension points for WEB UI


.. function:: initialize_extra_operators_links_plugins()
   Creates modules for loaded extension from extra operators links plugins


.. function:: integrate_executor_plugins() -> None
   Integrate executor plugins to the context.


.. function:: integrate_macros_plugins() -> None
   Integrates macro plugins.


