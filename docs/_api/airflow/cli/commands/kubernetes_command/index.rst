:mod:`airflow.cli.commands.kubernetes_command`
==============================================

.. py:module:: airflow.cli.commands.kubernetes_command

.. autoapi-nested-parse::

   Kubernetes sub-commands



Module Contents
---------------

.. function:: generate_pod_yaml(args)
   Generates yaml files for each task in the DAG. Used for testing output of KubernetesExecutor


.. function:: cleanup_pods(args)
   Clean up k8s pods in evicted/failed/succeeded states


.. function:: _delete_pod(name, namespace)
   Helper Function for cleanup_pods


