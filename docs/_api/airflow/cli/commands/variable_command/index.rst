:mod:`airflow.cli.commands.variable_command`
============================================

.. py:module:: airflow.cli.commands.variable_command

.. autoapi-nested-parse::

   Variable subcommands



Module Contents
---------------

.. function:: variables_list(args)
   Displays all of the variables


.. function:: variables_get(args)
   Displays variable by a given name


.. function:: variables_set(args)
   Creates new variable with a given name and value


.. function:: variables_delete(args)
   Deletes variable by a given name


.. function:: variables_import(args)
   Imports variables from a given file


.. function:: variables_export(args)
   Exports all of the variables to the file


.. function:: _import_helper(filepath)
   Helps import variables from the file


.. function:: _variable_export_helper(filepath)
   Helps export all of the variables to the file


