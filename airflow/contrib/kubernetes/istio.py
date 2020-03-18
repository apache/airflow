# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from kubernetes.stream import stream
try:
    from packaging.version import parse as semantic_version
except ImportError:
    # Python 2
    from distutils.version import LooseVersion as semantic_version  # type: ignore


class SidecarNames:
    """ Define strings that indicate container names
    """
    ISTIO_PROXY = 'istio-proxy'


class Istio(LoggingMixin):
    """ Handle all Istio-related logic
    """

    def __init__(self, kube_client):
        super(Istio, self).__init__()
        self._client = kube_client

    def handle_istio_proxy(self, pod):
        """If an istio-proxy sidecar is detected, and all other containers
        are terminated, then attempt to cleanly shutdown the sidecar.
        If we detect a version of Istio before it's compatible with Kubernetes
        Jobs, then raise an informative error message.

        Args:
            pod (V1Pod): The pod which we are checking for the sidecar

        Returns:
            (bool): True if we detect and exit istio-proxy,
                    False if we do not detect istio-proxy

        Raises:
            AirflowException: if we find an istio-proxy, and we can't shut it down.
        """
        if self._should_shutdown_istio_proxy(pod):
            self.log.info("Detected that a task finished and needs " +
                          "an istio-proxy sidecar to be cleaned up. " +
                          "pod name: {}".format(pod.metadata.name))
            self._shutdown_istio_proxy(pod)
            return True
        return False

    def _should_shutdown_istio_proxy(self, pod):
        """Look for an istio-proxy, and decide if it should be shutdown.

        Args:
            pod (V1Pod): The pod which we are checking for the sidecar

        Returns:
            (bool): True if we detect istio-proxy, and all other containers
                    are finished running, otherwise false
        """
        if pod.status.phase != "Running":
            return False
        found_istio = False
        for container_status in pod.status.container_statuses:
            if container_status.name == SidecarNames.ISTIO_PROXY and \
                    container_status.state.running:
                found_istio = True
                continue
            if not container_status.state.terminated:
                # Any state besides 'terminated' should be
                # considered still busy
                return False
        # If we didn't find istio at all, then we should
        # not shut it down. Also we should only shut it down
        # if it has state "running".
        return found_istio

    def _shutdown_istio_proxy(self, pod):
        """Shutdown the istio-proxy on the provided pod

        Args:
            pod (V1Pod): The pod which the container is in

        Returns:
            None

        Raises:
            AirflowException: if we find an istio-proxy, and we can't shut it down.
        """
        for container in pod.spec.containers:

            # Skip unless it's a sidecar named as SidecarNames.ISTIO_PROXY.
            if container.name != SidecarNames.ISTIO_PROXY:
                continue

            # Check if supported version of istio-proxy.
            # If we can't tell the version, proceed anyways.
            if ":" in container.image:
                _, tag = container.image.split(":")
                if semantic_version(tag) < semantic_version("1.3.0-rc.0"):
                    raise AirflowException(
                        'Please use istio version 1.3.0+ for KubernetesExecutor compatibility.' +
                        ' Detected version {}'.format(tag))

            # Determine the istio-proxy statusPort,
            # which is where /quitquitquit is implemented.
            # Default to 15020.
            status_port = "15020"
            for i in range(len(container.args)):
                arg = container.args[i]
                if arg.strip() == "--statusPort":
                    status_port = container.args[i + 1].strip()
                    break
                if arg.strip()[:13] == "--statusPort=":
                    status_port = arg.strip()[13:]
                    break

            self.log.info("Shutting down istio-proxy in pod {}".format(pod.metadata.name))
            self._post_quitquitquit(pod, container, status_port)

    def _post_quitquitquit(self, pod, container, status_port):
        """ Send the curl to shutdown the isto-proxy container
        """
        # Use exec to curl localhost inside of the sidecar.
        _ = stream(
            self._client.connect_get_namespaced_pod_exec,
            pod.metadata.name,
            pod.metadata.namespace,
            tty=False,
            stderr=True,
            stdin=False,
            stdout=True,
            container=container.name,
            command=[
                '/bin/sh',
                '-c',
                'curl -XPOST http://127.0.0.1:{}/quitquitquit'.format(status_port)])
