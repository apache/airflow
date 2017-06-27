import yaml
from kubernetes import client, config
from airflow.utils.state import State


class KubernetesHelper(object):
    def __init__(self):
        config.load_incluster_config()
        self.api = client.BatchV1Api()

    def launch_job(self, pod_info, namespace):
        dep = yaml.load(pod_info)
        resp = self.api.create_namespaced_job(body=dep, namespace=namespace)
        return resp

    def get_status(self, pod_id, namespace):
        return self.api.read_namespaced_job(pod_id, namespace).status

    def delete_job(self, job_id, namespace):
        body = client.V1DeleteOptions()
        self.api.delete_namespaced_job(name=job_id, namespace=namespace, body=body)
