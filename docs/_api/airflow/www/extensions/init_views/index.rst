:mod:`airflow.www.extensions.init_views`
========================================

.. py:module:: airflow.www.extensions.init_views


Module Contents
---------------

.. data:: log
   

   

.. data:: ROOT_APP_DIR
   

   

.. function:: init_flash_views(app)
   Init main app view - redirect to FAB


.. function:: init_appbuilder_views(app)
   Initialize Web UI views


.. function:: init_plugins(app)
   Integrate Flask and FAB with plugins


.. function:: init_error_handlers(app: Flask)
   Add custom errors handlers


.. function:: init_api_connexion(app: Flask) -> None
   Initialize Stable API


.. function:: init_api_experimental(app)
   Initialize Experimental API


