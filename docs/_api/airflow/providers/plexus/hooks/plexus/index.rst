:mod:`airflow.providers.plexus.hooks.plexus`
============================================

.. py:module:: airflow.providers.plexus.hooks.plexus


Module Contents
---------------

.. py:class:: PlexusHook

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Used for jwt token generation and storage to
   make Plexus API calls. Requires email and password
   Airflow variables be created.

   Example:
       - export AIRFLOW_VAR_EMAIL = user@corescientific.com
       - export AIRFLOW_VAR_PASSWORD = *******

   .. attribute:: token
      

      Returns users token


   
   .. method:: _generate_token(self)




