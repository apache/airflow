:mod:`airflow.cli.commands.db_command`
======================================

.. py:module:: airflow.cli.commands.db_command

.. autoapi-nested-parse::

   Database sub-commands



Module Contents
---------------

.. function:: initdb(args)
   Initializes the metadata database


.. function:: resetdb(args)
   Resets the metadata database


.. function:: upgradedb(args)
   Upgrades the metadata database


.. function:: check_migrations(args)
   Function to wait for all airflow migrations to complete. Used for launching airflow in k8s


.. function:: shell(args)
   Run a shell that allows to access metadata database


.. function:: check(_)
   Runs a check command that checks if db is available.


