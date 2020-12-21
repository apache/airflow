:mod:`airflow.mypy.plugin.decorators`
=====================================

.. py:module:: airflow.mypy.plugin.decorators


Module Contents
---------------

.. data:: TYPED_DECORATORS
   

   

.. py:class:: TypedDecoratorPlugin

   Bases: :class:`mypy.plugin.Plugin`

   Mypy plugin for typed decorators.

   
   .. method:: get_function_hook(self, fullname: str)

      Check for known typed decorators by name.




.. function:: _analyze_decorator(function_ctx: FunctionContext, provided_arguments: List[str])

.. function:: _change_decorator_function_type(decorated: CallableType, decorator: CallableType, provided_arguments: List[str]) -> CallableType

.. function:: plugin(version: str)
   Mypy plugin entrypoint.


