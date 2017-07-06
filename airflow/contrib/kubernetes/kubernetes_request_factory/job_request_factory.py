import yaml
import kubernetes_request_factory as req_factory


class SimpleJobRequestFactory(req_factory.KubernetesRequestFactory):
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
        req_factory.extract_name(pod, req)
        req_factory.extract_labels(pod, req)
        req_factory.extract_image(pod, req)
        req_factory.extract_cmds(pod, req)
        if len(pod.node_selectors) > 0:
            req_factory.extract_node_selector(pod, req)
        req_factory.extract_secrets(pod, req)
        req_factory.attach_volume_mounts(req)
        return req



