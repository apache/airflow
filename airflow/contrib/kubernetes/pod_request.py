import logging
import os
import time
import yaml
from abc import ABCMeta, abstractmethod
from airflow import AirflowException
import json
from airflow import dag_importer


class KubernetesRequestFactory():
    """
        Create requests to be sent to kube API. Extend this class
        to talk to kubernetes and generate your specific resources.
        This is equivalent of generating yaml files that can be used
        by `kubectl`
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def create(self, pod):
        """
            Creates the request for kubernetes API.

            :param pod: The pod object
        """
        pass


class SimpleJobRequestFactory(KubernetesRequestFactory):
    """
        Request generator for a simple pod.
    """


    _yaml = """apiVersion: batch/v1
kind: Job
metadata:
  name: name
spec:
  template:
    metadata:
      name: name
    spec:
      containers:
      - name: base
        image: airflow-slave:latest
        imagePullPolicy: Never
        command: ["/usr/local/airflow/entrypoint.sh", "/bin/bash sleep 25"]
        volumeMounts:
          - name: shared-data
            mountPath: "/usr/local/airflow/dags"
      restartPolicy: Never
    """

    def create(self, pod):
        req = yaml.load(self._yaml)
        req['metadata']['name'] = pod.name
        for k in pod.labels.keys():
            req['metadata']['labels'][k] = pod.labels[k]
        req['spec']['template']['spec']['containers'][0]['image'] = pod.image
        req['spec']['template']['spec']['containers'][0]['command'] = pod.cmds
        if len(pod.node_selectors) > 0:
            req['spec']['nodeSelector'] = pod.node_selectors
        env_secrets = [s for s in pod.secrets if s.deploy_type == 'env']
        if len(pod.envs) > 0 or len(env_secrets) > 0:
            env = []
            for k in pod.envs.keys():
                env.append({'name': k, 'value': pod.envs[k]})
            for secret in env_secrets:
                env.append({
                    'name': secret.deploy_target,
                    'valueFrom': {
                        'secretKeyRef': {
                            'name': secret.secret,
                            'key': secret.key
                        }
                    }
                })
            req['spec']['containers'][0]['env'] = env
        logging.info("using file mount {}".format(dag_importer.dag_import_spec))
        req['spec']['template']['spec']['volumes'] = [dag_importer.dag_import_spec]
        return req


class SimplePodRequestFactory(KubernetesRequestFactory):
    """
        Request generator for a simple pod.
    """
    _yaml = """apiVersion: v1
kind: Pod
metadata:
  name: job_id
  labels: 
      type: airflow
spec:
      containers:
      - name: base
        image: image
        command: ["/usr/entrypoint"]
      restartPolicy: Never
"""

    def create(self, pod):
        req = yaml.load(self._yaml)
        req['metadata']['name'] = pod.name
        for k in pod.labels.keys():
            req['metadata']['labels'][k] = pod.labels[k]
        req['spec']['containers'][0]['image'] = pod.image
        req['spec']['containers'][0]['command'] = pod.cmds
        if len(pod.node_selectors) > 0:
            req['spec']['nodeSelector'] = pod.node_selectors
        env_secrets = [s for s in pod.secrets if s.deploy_type == 'env']
        if len(pod.envs) > 0 or len(env_secrets) > 0:
            env = []
            for k in pod.envs.keys():
                env.append({'name': k, 'value': pod.envs[k]})
            for secret in env_secrets:
                env.append({
                    'name': secret.deploy_target,
                    'valueFrom': {
                        'secretKeyRef': {
                            'name': secret.secret,
                            'key': secret.key
                        }
                    }
                })
            req['spec']['containers'][0]['env'] = env
        vol_secrets = [s for s in pod.secrets if s.deploy_type == 'volume']
        if any(vol_secrets):
            req['spec']['containers'][0]['volumeMounts'] = []
            req['spec']['volumes'] = []
        for idx, vol in enumerate(vol_secrets):
            vol_id = 'secretvol' + str(idx)
            req['spec']['containers'][0]['volumeMounts'].append({
                'mountPath': vol.deploy_target,
                'name': vol_id,
                'readOnly': True
            })
            req['spec']['volumes'].append({
                'name': vol_id,
                'secret': {
                    'secretName': vol.secret
                }
            })
        return req


class ReturnValuePodRequestFactory(SimplePodRequestFactory):
    """
    Pod request factory with a PreStop hook to upload return value
    to the system's etcd service.
    :param kube_com_service_factory: Kubernetes Communication Service factory
    :type kube_com_service_factory: () => KubernetesCommunicationService
    """

    def __init__(self, kube_com_service_factory, result_data_file):
        super(ReturnValuePodRequestFactory, self).__init__()
        self._kube_com_service_factory = kube_com_service_factory
        self._result_data_file = result_data_file

    def after_create(self, body, pod):
        """
            Augment the pod with hyper-parameterized specific logic
            Adds a Kubernetes PreStop hook to upload the model training
            metrics to the Kubernetes communication engine (probably
            an etcd service running with airflow)
        """
        container = body['spec']['containers'][0]
        pre_stop_hook = self._kube_com_service_factory().pod_pre_stop_hook(self._result_data_file, pod.name)
        # Pre-stop hook only works on containers that are deleted. If the container
        # naturally exists there would be no pre-stop hook execution. Therefore we
        # simulate the hook by wrapping the exe command inside a script
        if "'" in ' '.join(container['command']):
            raise AirflowException('Please do not include single quote in your command for hyperparameterized pods')
        cmd = ' '.join(["'" + c + "'" if " " in c else c for c in container['command']])
        container['command'] = ['/bin/bash', '-c', "({}) ; ({})".format(cmd, pre_stop_hook)]
