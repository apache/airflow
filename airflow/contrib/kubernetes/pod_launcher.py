from airflow.contrib.kubernetes.pod import Pod
from airflow.contrib.kubernetes.kubernetes_request_factory import SimplePodRequestFactory
from kubernetes import config, client, watch
from airflow.utils.state import State
import json
import logging


class PodLauncher:
    def __init__(self):
        self.kube_req_factory = SimplePodRequestFactory()
        self._client = self._kube_client()
        self._watch = watch.Watch()
        self.logger = logging.getLogger(__name__)

    def run_pod(self, pod):
        # type: (Pod) -> State
        """
            Launches the pod synchronously and waits for completion.
        """

        req = self.kube_req_factory.create(self)
        print(json.dumps(req))
        resp = self._client.create_namespaced_pod(body=req, namespace=pod.namespace)
        self.logger.info("Job created. status='%s', yaml:\n%s"
                         % (str(resp.status), str(req)))
        final_status = self._monitor_pod(pod)
        return final_status

    def _kube_client(self):
        config.load_incluster_config()
        return client.CoreV1Api()

    def _monitor_pod(self, pod):
        # type: (Pod) -> State
        for event in self._watch.stream(self.read_pod(pod), pod.namespace):
            task = event['object']
            self.logger.info(
                "Event: {} had an event of type {}".format(task.metadata.name,
                                                           event['type']))
            status = self.process_status(task.metadata.name, task.status.phase)
            if status == State.SUCCESS or status == State.FAILED:
                return status

    def read_pod(self, pod):
        return self._client.read_namespaced_pod(pod.name, pod.namespace)

    def process_status(self, job_id, status):
        if status == 'Pending':
            return State.QUEUED
        elif status == 'Failed':
            self.logger.info("Event: {} Failed".format(job_id))
            return State.FAILED
        elif status == 'Succeeded':
            self.logger.info("Event: {} Succeeded".format(job_id))
            return State.SUCCESS
        elif status == 'Running':
            return State.RUNNING
        else:
            self.logger.info("Event: Invalid state {} on job {}".format(status, job_id))
            return State.FAILED
