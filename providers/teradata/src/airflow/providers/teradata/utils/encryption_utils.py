#
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

import secrets
import shlex
import string
import subprocess


def generate_random_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    password = "".join(secrets.choice(characters) for _ in range(length))
    return password


def generate_encrypted_file_with_openssl(file_path: str, password: str, out_file: str):
    cmd = [
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
    ]
    # Pass the passphrase on stdin rather than the command line so it is not
    # visible in the local host's process list (ps).
    subprocess.run(cmd, input=f"{password}\n".encode(), check=True)


def decrypt_remote_file_to_string(ssh_client, remote_enc_file, password, bteq_command_str):
    # Use -pass stdin to avoid shell quoting on any OS and keep the passphrase
    # out of the remote process table where ps could expose it.
    decrypt_cmd = (
        f"openssl enc -d -aes-256-cbc -salt -pbkdf2 -pass stdin -in {shlex.quote(remote_enc_file)} | "
        + bteq_command_str
    )
    stdin, stdout, stderr = ssh_client.exec_command(decrypt_cmd)
    stdin.write(password + "\n")
    stdin.flush()
    stdin.channel.shutdown_write()
    exit_status = stdout.channel.recv_exit_status()
    output = stdout.read().decode()
    err = stderr.read().decode()
    return exit_status, output, err
