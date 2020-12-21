:mod:`airflow.providers.grpc.hooks.grpc`
========================================

.. py:module:: airflow.providers.grpc.hooks.grpc

.. autoapi-nested-parse::

   GRPC Hook



Module Contents
---------------

.. py:class:: GrpcHook(grpc_conn_id: str, interceptors: Optional[List[Callable]] = None, custom_connection_func: Optional[Callable] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   General interaction with gRPC servers.

   :param grpc_conn_id: The connection ID to use when fetching connection info.
   :type grpc_conn_id: str
   :param interceptors: a list of gRPC interceptor objects which would be applied
       to the connected gRPC channel. None by default.
   :type interceptors: a list of gRPC interceptors based on or extends the four
       official gRPC interceptors, eg, UnaryUnaryClientInterceptor,
       UnaryStreamClientInterceptor, StreamUnaryClientInterceptor,
       StreamStreamClientInterceptor.
   :param custom_connection_func: The customized connection function to return gRPC channel.
   :type custom_connection_func: python callable objects that accept the connection as
       its only arg. Could be partial or lambda.

   
   .. method:: get_conn(self)



   
   .. method:: run(self, stub_class: Callable, call_func: str, streaming: bool = False, data: Optional[dict] = None)

      Call gRPC function and yield response to caller



   
   .. method:: _get_field(self, field_name: str)

      Fetches a field from extras, and returns it. This is some Airflow
      magic. The grpc hook type adds custom UI elements
      to the hook page, which allow admins to specify scopes, credential pem files, etc.
      They get formatted as shown below.




