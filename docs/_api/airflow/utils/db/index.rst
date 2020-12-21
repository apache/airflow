:mod:`airflow.utils.db`
=======================

.. py:module:: airflow.utils.db


Module Contents
---------------

.. data:: log
   

   

.. function:: merge_conn(conn, session=None)
   Add new Connection.


.. function:: add_default_pool_if_not_exists(session=None)
   Add default pool if it does not exist.


.. function:: create_default_connections(session=None)
   Create default Airflow connections.


.. function:: initdb()
   Initialize Airflow database.


.. function:: _get_alembic_config()

.. function:: check_migrations(timeout)
   Function to wait for all airflow migrations to complete.
   @param timeout:
   @return:


.. function:: upgradedb()
   Upgrade the database.


.. function:: resetdb()
   Clear out the database


.. function:: drop_airflow_models(connection)
   Drops all airflow models.
   @param connection:
   @return: None


.. function:: drop_flask_models(connection)
   Drops all Flask models.
   @param connection:
   @return:


.. function:: check(session=None)
   Checks if the database works.
   :param session: session of the sqlalchemy


