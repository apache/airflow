:mod:`airflow.sensors.python`
=============================

.. py:module:: airflow.sensors.python


Module Contents
---------------

.. py:class:: PythonSensor(*, python_callable: Callable, op_args: Optional[List] = None, op_kwargs: Optional[Dict] = None, templates_dict: Optional[Dict] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a Python callable to return True.

   User could put input argument in templates_dict
   e.g ``templates_dict = {'start_ds': 1970}``
   and access the argument by calling ``kwargs['templates_dict']['start_ds']``
   in the callable

   :param python_callable: A reference to an object that is callable
   :type python_callable: python callable
   :param op_kwargs: a dictionary of keyword arguments that will get unpacked
       in your function
   :type op_kwargs: dict
   :param op_args: a list of positional arguments that will get unpacked when
       calling your callable
   :type op_args: list
   :param templates_dict: a dictionary where the values are templates that
       will get templated by the Airflow engine sometime between
       ``__init__`` and ``execute`` takes place and are made available
       in your callable's context after the template has been applied.
   :type templates_dict: dict of str

   .. attribute:: template_fields
      :annotation: = ['templates_dict', 'op_args', 'op_kwargs']

      

   
   .. method:: poke(self, context: Dict)




