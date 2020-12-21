:mod:`airflow.secrets.environment_variables`
============================================

.. py:module:: airflow.secrets.environment_variables

.. autoapi-nested-parse::

   Objects relating to sourcing connections from environment variables



Module Contents
---------------

.. data:: CONN_ENV_PREFIX
   :annotation: = AIRFLOW_CONN_

   

.. data:: VAR_ENV_PREFIX
   :annotation: = AIRFLOW_VAR_

   

.. py:class:: EnvironmentVariablesBackend

   Bases: :class:`airflow.secrets.BaseSecretsBackend`

   Retrieves Connection object from environment variable.

   
   .. method:: get_conn_uri(self, conn_id: str)



   
   .. method:: get_variable(self, key: str)

      Get Airflow Variable from Environment Variable

      :param key: Variable Key
      :return: Variable Value




