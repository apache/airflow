"""
Module dedicated to common operations regarding using gcloud
"""

import json
import logging
import os
import signal
import tempfile
from contextlib import contextmanager
from subprocess import check_output
from unittest import mock

from google.auth import _cloud_sdk
from google.auth.environment_vars import CREDENTIALS, CLOUD_SDK_CONFIG_DIR

from utils.network_utils import find_free_port
from utils.process_utils import (
    execute_in_subprocess,
    start_port_forwarding_process,
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

MAX_TIME_TO_OPEN_ROUTING = 90


# function based on method of GoogleBaseHook of the same name taken from apache/airflow
@contextmanager
def provide_authorized_gcloud(project_id: str) -> None:
    """
    Provides a separate gcloud configuration with current credentials.
    The gcloud allows you to login to GCP only - ``gcloud auth login`` and
    for the needs of Application Default Credentials ``gcloud auth application-default login``.
    In our case, we want all commands to use only the credentials from ADCm so
    we need to configure the credentials in gcloud manually.

    :param project_id: Google Cloud project id.
    :type project_id: str
    """
    credentials_path = _cloud_sdk.get_application_default_credentials_path()

    with tempfile.TemporaryDirectory() as gcloud_config_tmp, mock.patch.dict(
        "os.environ", {CLOUD_SDK_CONFIG_DIR: gcloud_config_tmp}
    ):

        # added redirecting stderr to devnull
        # to avoid printing refresh token credentials, among others
        with open(os.devnull, "w") as devnull:
            if CREDENTIALS in os.environ:
                # This solves most cases when we are logged in using the service key in Airflow.
                # Don't display stdout/stderr for security reason
                check_output(
                    [
                        "gcloud",
                        "auth",
                        "activate-service-account",
                        f"--key-file={os.environ[CREDENTIALS]}",
                    ],
                    stderr=devnull,
                )
            elif os.path.exists(credentials_path):
                # If we are logged in by `gcloud auth application-default`
                # then we need to log in manually.
                # This will make the `gcloud auth application-default`
                # and `gcloud auth` credentials equals.
                with open(credentials_path) as creds_file:
                    creds_content = json.loads(creds_file.read())
                    # Don't display stdout/stderr for security reason
                    check_output(
                        [
                            "gcloud",
                            "config",
                            "set",
                            "auth/client_id",
                            creds_content["client_id"],
                        ],
                        stderr=devnull,
                    )
                    # Don't display stdout/stderr for security reason
                    check_output(
                        [
                            "gcloud",
                            "config",
                            "set",
                            "auth/client_secret",
                            creds_content["client_secret"],
                        ],
                        stderr=devnull,
                    )
                    # Don't display stdout/stderr for security reason
                    check_output(
                        [
                            "gcloud",
                            "auth",
                            "activate-refresh-token",
                            creds_content["client_id"],
                            creds_content["refresh_token"],
                        ],
                        stderr=devnull,
                    )
            if project_id:
                # Don't display stdout/stderr for security reason
                check_output(
                    ["gcloud", "config", "set", "core/project", project_id],
                    stderr=devnull,
                )
        # we need to set CREDENTIALS variable, because ADC won't be able to use gcloud's
        # application default credentials, as we have changed gcloud configuration
        if CREDENTIALS not in os.environ and os.path.exists(credentials_path):
            with mock.patch.dict("os.environ", {CREDENTIALS: credentials_path}):
                yield
        else:
            yield


@contextmanager
def open_routing_to_gke_node(node_name: str, zone: str) -> int:
    """
    Context manager that opens dynamic port forwarding to the node on port A
    (in the background) and then starts a background https proxy process converting http requests
    into socks requests and redirecting them from port B to port A.

    :param node_name: name of the node to connect to.
    :type node_name: str
    :param zone: zone where the node is located.
    :type zone: str

    :yields: port on which http proxy process is listening on.
    :type: int

    :raises: ValueError: if node_name was not provided.
    """

    if not node_name:
        raise ValueError("No node provided.")

    socks_port = find_free_port()
    http_port = find_free_port()

    with tempfile.TemporaryDirectory() as ssh_key_tmp_dir:

        ssh_proc = None
        hpts_proc = None

        try:
            key_path = os.path.join(ssh_key_tmp_dir, "key")

            ssh_command = [
                "gcloud",
                "compute",
                "ssh",
                node_name,
                f"--zone={zone}",
                f"--ssh-key-file={key_path}",
                "--quiet",
                "--ssh-flag=-N",
                "--ssh-flag=-vvv",
                f"--ssh-flag=-D 127.0.0.1:{socks_port}",
            ]

            log.info(
                "Opening dynamic port forwarding to node %s on port: %d",
                node_name,
                socks_port,
            )

            ssh_proc = start_port_forwarding_process(ssh_command, socks_port, MAX_TIME_TO_OPEN_ROUTING)

            log.info("Dynamic port forwarding is open.")

            hpts_command = [
                "npx",
                "hpts",
                "-s",
                f"127.0.0.1:{socks_port}",
                "-p",
                f"{http_port}",
            ]

            log.info(
                "Starting a http proxy process listening on %d converting http requests "
                "into socks requests and redirecting them to %d",
                http_port,
                socks_port,
            )

            hpts_proc = start_port_forwarding_process(hpts_command, http_port, MAX_TIME_TO_OPEN_ROUTING)

            yield http_port

        finally:
            # terminate processes if they haven't finished already
            if ssh_proc is not None and ssh_proc.poll() is None:
                log.debug("Terminating dynamic port forwarding process.")
                os.kill(ssh_proc.pid, signal.SIGTERM)
            if hpts_proc is not None and hpts_proc.poll() is None:
                log.debug("Terminating http proxy process.")
                os.kill(hpts_proc.pid, signal.SIGTERM)


def get_cluster_credentials(project_id: str, zone: str, cluster_id: str, private_endpoint: bool) -> None:
    """
    Executes gcloud subcommand in order to update kubeconfig file with credentials
    of given GKE cluster.

    :param project_id: Google Cloud project the GKE cluster is located in.
    :type project_id: str
    :param zone: location of GKE cluster.
    :type zone: str
    :param cluster_id: id of GKE cluster.
    :type cluster_id: str
    :param private_endpoint: set to True if you want to set up connection
        to private master endpoint and False otherwise.
    :type private_endpoint: bool
    """

    command = [
        "gcloud",
        "container",
        "clusters",
        "get-credentials",
        cluster_id,
        "--zone",
        zone,
        "--project",
        project_id,
    ]

    if private_endpoint:
        command.append("--internal-ip")

    execute_in_subprocess(command)
