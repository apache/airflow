:mod:`airflow.kubernetes.kubernetes_helper_functions`
=====================================================

.. py:module:: airflow.kubernetes.kubernetes_helper_functions


Module Contents
---------------

.. function:: _strip_unsafe_kubernetes_special_chars(string: str) -> str
   Kubernetes only supports lowercase alphanumeric characters, "-" and "." in
   the pod name.
   However, there are special rules about how "-" and "." can be used so let's
   only keep
   alphanumeric chars  see here for detail:
   https://kubernetes.io/docs/concepts/overview/working-with-objects/names/

   :param string: The requested Pod name
   :return: Pod name stripped of any unsafe characters


.. function:: create_pod_id(dag_id: str, task_id: str) -> str
   Generates the kubernetes safe pod_id. Note that this is
   NOT the full ID that will be launched to k8s. We will add a uuid
   to ensure uniqueness.

   :param dag_id: DAG ID
   :param task_id: Task ID
   :return: The non-unique pod_id for this task/DAG pairing


