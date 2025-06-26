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
import string
import subprocess


def generate_random_password(length=12):
    # Define the character set: letters, digits, and special characters
    characters = string.ascii_letters + string.digits + string.punctuation
    # Generate a random password
    password = "".join(secrets.choice(characters) for _ in range(length))
    return password


def generate_encrypted_file_with_openssl(file_path: str, password: str, out_file: str):
    # Write plaintext temporarily to file

    # Run openssl enc with AES-256-CBC, pbkdf2, salt
    cmd = [
        "openssl",
        "enc",
        "-aes-256-cbc",
        "-salt",
        "-pbkdf2",
        "-pass",
        f"pass:{password}",
        "-in",
        file_path,
        "-out",
        out_file,
    ]
    subprocess.run(cmd, check=True)


def decrypt_remote_file_to_string(ssh_client, remote_enc_file, password, bteq_command_str):
    # Run openssl decrypt command on remote machine
    quoted_password = shell_quote_single(password)

    decrypt_cmd = (
        f"openssl enc -d -aes-256-cbc -salt -pbkdf2 -pass pass:{quoted_password} -in {remote_enc_file} | "
        + bteq_command_str
    )
    # Clear password to prevent lingering sensitive data
    password = None
    quoted_password = None
    stdin, stdout, stderr = ssh_client.exec_command(decrypt_cmd)
    # Wait for command to finish
    exit_status = stdout.channel.recv_exit_status()
    output = stdout.read().decode()
    err = stderr.read().decode()
    return exit_status, output, err


def shell_quote_single(s):
    # Escape single quotes in s, then wrap in single quotes
    # In shell, to include a single quote inside single quotes, close, add '\'' and reopen
    return "'" + s.replace("'", "'\\''") + "'"
