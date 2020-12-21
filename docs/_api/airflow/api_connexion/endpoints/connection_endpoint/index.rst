:mod:`airflow.api_connexion.endpoints.connection_endpoint`
==========================================================

.. py:module:: airflow.api_connexion.endpoints.connection_endpoint


Module Contents
---------------

.. function:: delete_connection(connection_id, session)
   Delete a connection entry


.. function:: get_connection(connection_id, session)
   Get a connection entry


.. function:: get_connections(session, limit, offset=0)
   Get all connection entries


.. function:: patch_connection(connection_id, session, update_mask=None)
   Update a connection entry


.. function:: post_connection(session)
   Create connection entry


