:mod:`airflow.providers.cncf.kubernetes.operators.spark_kubernetes`
===================================================================

.. py:module:: airflow.providers.cncf.kubernetes.operators.spark_kubernetes


Module Contents
---------------

.. py:class:: SparkKubernetesOperator(*, application_file: str, namespace: Optional[str] = None, kubernetes_conn_id: str = 'kubernetes_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates sparkApplication object in kubernetes cluster:

   .. seealso::
       For more detail about Spark Application Object have a look at the reference:
       https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

   :param application_file: filepath to kubernetes custom_resource_definition of sparkApplication
   :type application_file:  str
   :param namespace: kubernetes namespace to put sparkApplication
   :type namespace: str
   :param kubernetes_conn_id: the connection to Kubernetes cluster
   :type kubernetes_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['application_file', 'namespace']

      

   .. attribute:: template_ext
      :annotation: = ['yaml', 'yml', 'json']

      

   .. attribute:: ui_color
      :annotation: = #f4a460

      

   
   .. method:: execute(self, context)




