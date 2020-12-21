:mod:`airflow.providers.cncf.kubernetes.sensors.spark_kubernetes`
=================================================================

.. py:module:: airflow.providers.cncf.kubernetes.sensors.spark_kubernetes


Module Contents
---------------

.. py:class:: SparkKubernetesSensor(*, application_name: str, attach_log: bool = False, namespace: Optional[str] = None, kubernetes_conn_id: str = 'kubernetes_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks sparkApplication object in kubernetes cluster:

   .. seealso::
       For more detail about Spark Application Object have a look at the reference:
       https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

   :param application_name: spark Application resource name
   :type application_name:  str
   :param namespace: the kubernetes namespace where the sparkApplication reside in
   :type namespace: str
   :param kubernetes_conn_id: the connection to Kubernetes cluster
   :type kubernetes_conn_id: str
   :param attach_log: determines whether logs for driver pod should be appended to the sensor log
   :type attach_log: bool

   .. attribute:: template_fields
      :annotation: = ['application_name', 'namespace']

      

   .. attribute:: FAILURE_STATES
      :annotation: = ['FAILED', 'UNKNOWN']

      

   .. attribute:: SUCCESS_STATES
      :annotation: = ['COMPLETED']

      

   
   .. method:: _log_driver(self, application_state: str)



   
   .. method:: poke(self, context: Dict)




