:mod:`airflow.task.task_runner.cgroup_task_runner`
==================================================

.. py:module:: airflow.task.task_runner.cgroup_task_runner

.. autoapi-nested-parse::

   Task runner for cgroup to run Airflow task



Module Contents
---------------

.. py:class:: CgroupTaskRunner(local_task_job)

   Bases: :class:`airflow.task.task_runner.base_task_runner.BaseTaskRunner`

   Runs the raw Airflow task in a cgroup that has containment for memory and
   cpu. It uses the resource requirements defined in the task to construct
   the settings for the cgroup.

   Cgroup must be mounted first otherwise CgroupTaskRunner
   will not be able to work.

   cgroup-bin Ubuntu package must be installed to use cgexec command.

   Note that this task runner will only work if the Airflow user has root privileges,
   e.g. if the airflow user is called `airflow` then the following entries (or an even
   less restrictive ones) are needed in the sudoers file (replacing
   /CGROUPS_FOLDER with your system's cgroups folder, e.g. '/sys/fs/cgroup/'):
   airflow ALL= (root) NOEXEC: /bin/chown /CGROUPS_FOLDER/memory/airflow/*
   airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/memory/airflow/*..*
   airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/memory/airflow/* *
   airflow ALL= (root) NOEXEC: /bin/chown /CGROUPS_FOLDER/cpu/airflow/*
   airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/cpu/airflow/*..*
   airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/cpu/airflow/* *
   airflow ALL= (root) NOEXEC: /bin/chmod /CGROUPS_FOLDER/memory/airflow/*
   airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/memory/airflow/*..*
   airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/memory/airflow/* *
   airflow ALL= (root) NOEXEC: /bin/chmod /CGROUPS_FOLDER/cpu/airflow/*
   airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/cpu/airflow/*..*
   airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/cpu/airflow/* *

   
   .. method:: _create_cgroup(self, path)

      Create the specified cgroup.

      :param path: The path of the cgroup to create.
      E.g. cpu/mygroup/mysubgroup
      :return: the Node associated with the created cgroup.
      :rtype: cgroupspy.nodes.Node



   
   .. method:: _delete_cgroup(self, path)

      Delete the specified cgroup.

      :param path: The path of the cgroup to delete.
      E.g. cpu/mygroup/mysubgroup



   
   .. method:: start(self)



   
   .. method:: return_code(self)



   
   .. method:: terminate(self)



   
   .. method:: on_finish(self)



   
   .. staticmethod:: _get_cgroup_names()

      :return: a mapping between the subsystem name to the cgroup name
      :rtype: dict[str, str]




