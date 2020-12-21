:mod:`airflow.cli.commands.pool_command`
========================================

.. py:module:: airflow.cli.commands.pool_command

.. autoapi-nested-parse::

   Pools sub-commands



Module Contents
---------------

.. function:: _tabulate_pools(pools, tablefmt='fancy_grid')

.. function:: pool_list(args)
   Displays info of all the pools


.. function:: pool_get(args)
   Displays pool info by a given name


.. function:: pool_set(args)
   Creates new pool with a given name and slots


.. function:: pool_delete(args)
   Deletes pool by a given name


.. function:: pool_import(args)
   Imports pools from the file


.. function:: pool_export(args)
   Exports all of the pools to the file


.. function:: pool_import_helper(filepath)
   Helps import pools from the json file


.. function:: pool_export_helper(filepath)
   Helps export all of the pools to the json file


