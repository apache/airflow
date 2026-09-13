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
from __future__ import annotations

import string
from unittest.mock import MagicMock, patch

from airflow.providers.teradata.utils.encryption_utils import (
    decrypt_remote_file_to_string,
    generate_encrypted_file_with_openssl,
    generate_random_password,
)


class TestEncryptionUtils:
    def test_generate_random_password_length(self):
        pwd = generate_random_password(16)
        assert len(pwd) == 16
        allowed_chars = string.ascii_letters + string.digits + string.punctuation
        assert all(c in allowed_chars for c in pwd) is True

    @patch("subprocess.run")
    def test_generate_encrypted_file_with_openssl_calls_subprocess(self, mock_run):
        file_path = "/tmp/plain.txt"
        password = "testpass"
        out_file = "/tmp/encrypted.enc"

        generate_encrypted_file_with_openssl(file_path, password, out_file)

        mock_run.assert_called_once_with(
            [
                "openssl",
                "enc",
                "-aes-256-cbc",
                "-salt",
                "-pbkdf2",
                "-pass",
                "stdin",
                "-in",
                file_path,
                "-out",
                out_file,
            ],
            input=f"{password}\n".encode(),
            check=True,
        )

    @patch("subprocess.run")
    def test_generate_encrypted_file_passphrase_not_on_argv(self, mock_run):
        """The passphrase is fed on stdin, never placed on the command line (ps-visible)."""
        password = "s3cr3t;rm -rf ~"
        generate_encrypted_file_with_openssl("/tmp/plain.txt", password, "/tmp/out.enc")
        args, kwargs = mock_run.call_args
        cmd = args[0]
        assert "stdin" in cmd
        assert not any(password in str(part) for part in cmd), "passphrase leaked onto argv"
        assert kwargs["input"] == f"{password}\n".encode()

    def test_decrypt_remote_file_to_string(self):
        password = "mysecret"
        remote_enc_file = "/remote/encrypted.enc"
        bteq_command_str = "bteq -c UTF-8"

        ssh_client = MagicMock()
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_stdout.read.return_value = b"decrypted output"
        mock_stderr.read.return_value = b""
        ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        exit_status, output, err = decrypt_remote_file_to_string(
            ssh_client, remote_enc_file, password, bteq_command_str
        )

        expected_cmd = (
            f"openssl enc -d -aes-256-cbc -salt -pbkdf2 -pass stdin -in {remote_enc_file} | "
            + bteq_command_str
        )
        ssh_client.exec_command.assert_called_once_with(expected_cmd)
        mock_stdin.write.assert_called_once_with(password + "\n")
        mock_stdin.flush.assert_called_once()
        mock_stdin.channel.shutdown_write.assert_called_once()
        assert exit_status == 0
        assert output == "decrypted output"
        assert err == ""

    def test_decrypt_remote_file_passphrase_not_on_argv(self):
        """The passphrase is passed via stdin, never on the remote command line."""
        password = "s3cr3t&rm -rf ~"
        remote_enc_file = "/remote/encrypted.enc"
        ssh_client = MagicMock()
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_stdout.read.return_value = b""
        mock_stderr.read.return_value = b""
        ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        decrypt_remote_file_to_string(ssh_client, remote_enc_file, password, "bteq")

        cmd = ssh_client.exec_command.call_args[0][0]
        assert password not in cmd, "passphrase leaked onto remote command line"
        assert "-pass stdin" in cmd
