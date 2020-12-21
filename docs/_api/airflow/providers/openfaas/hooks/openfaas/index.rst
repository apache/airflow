:mod:`airflow.providers.openfaas.hooks.openfaas`
================================================

.. py:module:: airflow.providers.openfaas.hooks.openfaas


Module Contents
---------------

.. data:: OK_STATUS_CODE
   :annotation: = 202

   

.. py:class:: OpenFaasHook(function_name=None, conn_id: str = 'open_faas_default', *args, **kwargs)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interact with OpenFaaS to query, deploy, invoke and update function

   :param function_name: Name of the function, Defaults to None
   :type function_name: str
   :param conn_id: openfaas connection to use, Defaults to open_faas_default
       for example host : http://openfaas.faas.com, Conn Type : Http
   :type conn_id: str

   .. attribute:: GET_FUNCTION
      :annotation: = /system/function/

      

   .. attribute:: INVOKE_ASYNC_FUNCTION
      :annotation: = /async-function/

      

   .. attribute:: DEPLOY_FUNCTION
      :annotation: = /system/functions

      

   .. attribute:: UPDATE_FUNCTION
      :annotation: = /system/functions

      

   
   .. method:: get_conn(self)



   
   .. method:: deploy_function(self, overwrite_function_if_exist: bool, body: Dict[str, Any])

      Deploy OpenFaaS function



   
   .. method:: invoke_async_function(self, body: Dict[str, Any])

      Invoking function



   
   .. method:: update_function(self, body: Dict[str, Any])

      Update OpenFaaS function



   
   .. method:: does_function_exist(self)

      Whether OpenFaaS function exists or not




