from airflow import AirflowException
from kubernetes import client, config
from pod_request import SimplePodRequestFactory, KubernetesRequestFactory
import json
import logging

class Job:
    """
        Represents a kubernetes pod and manages execution of a single pod.
        :param image: The docker image
        :type image: str
        :param env: A dict containing the environment variables
        :type env: dict
        :param cmds: The command to be run on the pod
        :type cmd: list str
        :param secrets: Secrets to be launched to the pod
        :type secrets: list Secret
        :param result: The result that will be returned to the operator after
                       successful execution of the pod
        :type result: any

    """
    pod_timeout = 3600

    def __init__(
            self,
            image,
            envs={},
            cmds=[],
            secrets=[],
            labels={},
            node_selectors={},
            kube_req_factory=None,
            name=None,
            namespace='default',
            result=None):
        self.image = image
        self.envs = envs
        self.cmds = cmds
        self.secrets = secrets
        self.result = result
        self.labels = labels
        self.name = name
        self.node_selectors = node_selectors
        self.kube_req_factory = (kube_req_factory or SimplePodRequestFactory)()
        self.namespace = namespace
        self.logger = logging.getLogger(self.__class__.__name__)
        if not isinstance(self.kube_req_factory, KubernetesRequestFactory):
            raise AirflowException('`kube_req_factory`  should implement KubernetesRequestFactory')

    def launch(self):
        """
            Launches the pod synchronously and waits for completion.
        """
        k8s_beta = self._kube_client()
        req = self.kube_req_factory.create(self)
        print(json.dumps(req))
        resp = k8s_beta.create_namespaced_job(body=req, namespace=self.namespace)
        self.logger.info("Job created. status='%s', yaml:\n%s" % (str(resp.status), str(req)))

    def _kube_client(self):
        config.load_incluster_config()
        return client.BatchV1Api()

    def _execution_finished(self):
        k8s_beta = self._kube_client()
        resp = k8s_beta.read_namespaced_job_status(self.name, namespace=self.namespace)
        self.logger.info('status : ' + str(resp.status))
        if resp.status.phase == 'Failed':
            raise Exception("Job " + self.name + " failed!")
        return resp.status.phase != 'Running'
