import signal

from unittest import TestCase, mock

from utils.google_cloud.gcloud_utils import (
    MAX_TIME_TO_OPEN_ROUTING,
    get_cluster_credentials,
    open_routing_to_gke_node,
)

MODULE_NAME = "utils.google_cloud.gcloud_utils"


PROJECT_ID = "test_project_id"
ZONE = "test_zone"
CLUSTER_ID = "test_cluster_id"
NODE_NAME = "test_node_name"
TEMP_DIR = "temp_dir"
SOCKS_PORT = 2424
HTTP_PORT = 2423
SSH_PID = 1000
HPTS_PID = 1001


class TestGcloudUtils(TestCase):
    @mock.patch(MODULE_NAME + ".find_free_port")
    @mock.patch("tempfile.TemporaryDirectory")
    @mock.patch(MODULE_NAME + ".start_port_forwarding_process")
    @mock.patch("os.kill")
    def test_open_routing_to_gke_node(
        self,
        mock_os_kill,
        mock_start_port_forwarding_process,
        mock_temp_dir,
        mock_find_free_port,
    ):
        mock_temp_dir.return_value.__enter__.return_value = TEMP_DIR

        mock_find_free_port.side_effect = [SOCKS_PORT, HTTP_PORT]

        mock_start_port_forwarding_process.side_effect = [
            mock.MagicMock(pid=SSH_PID, **{"poll.return_value": None}),
            mock.MagicMock(pid=HPTS_PID, **{"poll.return_value": None}),
        ]

        with open_routing_to_gke_node(node_name=NODE_NAME, zone=ZONE) as http_port:
            self.assertEqual(http_port, HTTP_PORT)

        ssh_command = [
            "gcloud",
            "compute",
            "ssh",
            NODE_NAME,
            f"--zone={ZONE}",
            f"--ssh-key-file={TEMP_DIR}/key",
            "--quiet",
            "--ssh-flag=-N",
            "--ssh-flag=-vvv",
            f"--ssh-flag=-D 127.0.0.1:{SOCKS_PORT}",
        ]

        hpts_command = [
            "npx",
            "hpts",
            "-s",
            f"127.0.0.1:{SOCKS_PORT}",
            "-p",
            f"{HTTP_PORT}",
        ]

        self.assertEqual(mock_find_free_port.call_count, 2)
        self.assertEqual(mock_start_port_forwarding_process.call_count, 2)
        mock_start_port_forwarding_process.assert_any_call(ssh_command, SOCKS_PORT, MAX_TIME_TO_OPEN_ROUTING)
        mock_start_port_forwarding_process.assert_any_call(hpts_command, HTTP_PORT, MAX_TIME_TO_OPEN_ROUTING)
        self.assertEqual(mock_os_kill.call_count, 2)
        mock_os_kill.assert_any_call(SSH_PID, signal.SIGTERM)
        mock_os_kill.assert_any_call(HPTS_PID, signal.SIGTERM)

    def test_open_routing_to_gke_node_no_node_name(self):

        with self.assertRaises(ValueError):
            with open_routing_to_gke_node(node_name="", zone=ZONE):
                pass

    # pylint: disable=no-self-use
    @mock.patch(MODULE_NAME + ".execute_in_subprocess")
    def test_get_cluster_credentials(self, mock_execute_in_subprocess):
        get_cluster_credentials(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            private_endpoint=False,
        )

        mock_execute_in_subprocess.assert_called_once_with(
            [
                "gcloud",
                "container",
                "clusters",
                "get-credentials",
                CLUSTER_ID,
                "--zone",
                ZONE,
                "--project",
                PROJECT_ID,
            ]
        )

    @mock.patch(MODULE_NAME + ".execute_in_subprocess")
    def test_get_cluster_credentials_private(self, mock_execute_in_subprocess):
        get_cluster_credentials(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            private_endpoint=True,
        )

        mock_execute_in_subprocess.assert_called_once_with(
            [
                "gcloud",
                "container",
                "clusters",
                "get-credentials",
                CLUSTER_ID,
                "--zone",
                ZONE,
                "--project",
                PROJECT_ID,
                "--internal-ip",
            ]
        )
