:mod:`airflow.utils.cli_action_loggers`
=======================================

.. py:module:: airflow.utils.cli_action_loggers

.. autoapi-nested-parse::

   An Action Logger module. Singleton pattern has been applied into this module
   so that registered callbacks can be used all through the same python process.



Module Contents
---------------

.. function:: register_pre_exec_callback(action_logger)
   Registers more action_logger function callback for pre-execution.
   This function callback is expected to be called with keyword args.
   For more about the arguments that is being passed to the callback,
   refer to airflow.utils.cli.action_logging()

   :param action_logger: An action logger function
   :return: None


.. function:: register_post_exec_callback(action_logger)
   Registers more action_logger function callback for post-execution.
   This function callback is expected to be called with keyword args.
   For more about the arguments that is being passed to the callback,
   refer to airflow.utils.cli.action_logging()

   :param action_logger: An action logger function
   :return: None


.. function:: on_pre_execution(**kwargs)
   Calls callbacks before execution.
   Note that any exception from callback will be logged but won't be propagated.

   :param kwargs:
   :return: None


.. function:: on_post_execution(**kwargs)
   Calls callbacks after execution.
   As it's being called after execution, it can capture status of execution,
   duration, etc. Note that any exception from callback will be logged but
   won't be propagated.

   :param kwargs:
   :return: None


.. function:: default_action_log(log, **_)
   A default action logger callback that behave same as www.utils.action_logging
   which uses global session and pushes log ORM object.

   :param log: An log ORM instance
   :param **_: other keyword arguments that is not being used by this function
   :return: None


.. data:: __pre_exec_callbacks
   :annotation: :List[Callable] = []

   

.. data:: __post_exec_callbacks
   :annotation: :List[Callable] = []

   

