:mod:`airflow.cli.commands.user_command`
========================================

.. py:module:: airflow.cli.commands.user_command

.. autoapi-nested-parse::

   User sub-commands



Module Contents
---------------

.. function:: users_list(args)
   Lists users at the command line


.. function:: users_create(args)
   Creates new user in the DB


.. function:: users_delete(args)
   Deletes user from DB


.. function:: users_manage_role(args, remove=False)
   Deletes or appends user roles


.. function:: users_export(args)
   Exports all users to the json file


.. function:: users_import(args)
   Imports users from the json file


.. function:: _import_users(users_list)

.. data:: add_role
   

   

.. data:: remove_role
   

   

