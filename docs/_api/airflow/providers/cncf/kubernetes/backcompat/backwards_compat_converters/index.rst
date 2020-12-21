:mod:`airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters`
===============================================================================

.. py:module:: airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters

.. autoapi-nested-parse::

   Executes task in a Kubernetes POD



Module Contents
---------------

.. function:: _convert_kube_model_object(obj, old_class, new_class)

.. function:: convert_volume(volume) -> k8s.V1Volume
   Converts an airflow Volume object into a k8s.V1Volume

   :param volume:
   :return: k8s.V1Volume


.. function:: convert_volume_mount(volume_mount) -> k8s.V1VolumeMount
   Converts an airflow VolumeMount object into a k8s.V1VolumeMount

   :param volume_mount:
   :return: k8s.V1VolumeMount


.. function:: convert_resources(resources) -> k8s.V1ResourceRequirements
   Converts an airflow Resources object into a k8s.V1ResourceRequirements

   :param resources:
   :return: k8s.V1ResourceRequirements


.. function:: convert_port(port) -> k8s.V1ContainerPort
   Converts an airflow Port object into a k8s.V1ContainerPort

   :param port:
   :return: k8s.V1ContainerPort


.. function:: convert_env_vars(env_vars) -> List[k8s.V1EnvVar]
   Converts a dictionary into a list of env_vars

   :param env_vars:
   :return:


.. function:: convert_pod_runtime_info_env(pod_runtime_info_envs) -> k8s.V1EnvVar
   Converts a PodRuntimeInfoEnv into an k8s.V1EnvVar

   :param pod_runtime_info_envs:
   :return:


.. function:: convert_image_pull_secrets(image_pull_secrets) -> List[k8s.V1LocalObjectReference]
   Converts a PodRuntimeInfoEnv into an k8s.V1EnvVar

   :param image_pull_secrets:
   :return:


.. function:: convert_configmap(configmaps) -> k8s.V1EnvFromSource
   Converts a str into an k8s.V1EnvFromSource

   :param configmaps:
   :return:


