:mod:`airflow.www.app`
======================

.. py:module:: airflow.www.app


Module Contents
---------------

.. data:: app
   :annotation: :Optional[Flask]

   

.. data:: csrf
   

   

.. function:: sync_appbuilder_roles(flask_app)
   Sync appbuilder roles to DB


.. function:: create_app(config=None, testing=False, app_name='Airflow')
   Create a new instance of Airflow WWW app


.. function:: cached_app(config=None, testing=False)
   Return cached instance of Airflow WWW app


