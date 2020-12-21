:mod:`airflow.providers.cloudant.hooks.cloudant`
================================================

.. py:module:: airflow.providers.cloudant.hooks.cloudant

.. autoapi-nested-parse::

   Hook for Cloudant



Module Contents
---------------

.. py:class:: CloudantHook(cloudant_conn_id: str = 'cloudant_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interact with Cloudant. This class is a thin wrapper around the cloudant python library.

   .. seealso:: the latest documentation `here <https://python-cloudant.readthedocs.io/en/latest/>`_.

   :param cloudant_conn_id: The connection id to authenticate and get a session object from cloudant.
   :type cloudant_conn_id: str

   
   .. method:: get_conn(self)

      Opens a connection to the cloudant service and closes it automatically if used as context manager.

      .. note::
          In the connection form:
          - 'host' equals the 'Account' (optional)
          - 'login' equals the 'Username (or API Key)' (required)
          - 'password' equals the 'Password' (required)

      :return: an authorized cloudant session context manager object.
      :rtype: cloudant



   
   .. method:: _validate_connection(self, conn: cloudant)




