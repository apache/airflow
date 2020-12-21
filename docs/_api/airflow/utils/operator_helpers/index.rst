:mod:`airflow.utils.operator_helpers`
=====================================

.. py:module:: airflow.utils.operator_helpers


Module Contents
---------------

.. data:: AIRFLOW_VAR_NAME_FORMAT_MAPPING
   

   

.. function:: context_to_airflow_vars(context, in_env_var_format=False)
   Given a context, this function provides a dictionary of values that can be used to
   externally reconstruct relations between dags, dag_runs, tasks and task_instances.
   Default to abc.def.ghi format and can be made to ABC_DEF_GHI format if
   in_env_var_format is set to True.

   :param context: The context for the task_instance of interest.
   :type context: dict
   :param in_env_var_format: If returned vars should be in ABC_DEF_GHI format.
   :type in_env_var_format: bool
   :return: task_instance context as dict.


.. function:: determine_kwargs(func: Callable, args: Union[Tuple, List], kwargs: Dict) -> Dict
   Inspect the signature of a given callable to determine which arguments in kwargs need
   to be passed to the callable.

   :param func: The callable that you want to invoke
   :param args: The positional arguments that needs to be passed to the callable, so we
       know how many to skip.
   :param kwargs: The keyword arguments that need to be filtered before passing to the callable.
   :return: A dictionary which contains the keyword arguments that are compatible with the callable.


.. function:: make_kwargs_callable(func: Callable) -> Callable
   Make a new callable that can accept any number of positional or keyword arguments
   but only forwards those required by the given callable func.


