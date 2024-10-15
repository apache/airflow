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

import base64
import os
from tempfile import TemporaryDirectory

import pytest
from tests_common.test_utils.gcp_system_helpers import GoogleSystemTest, provide_gcp_context

from airflow.providers.google.cloud.hooks.kms import CloudKMSHook

from providers.tests.google.cloud.utils.gcp_authenticator import GCP_KMS_KEY

# To prevent resource name collisions, key ring and key resources CANNOT be deleted, so
# to avoid cluttering the project, we only create the key once during project initialization.
# See: https://cloud.google.com/kms/docs/faq#cannot_delete
GCP_KMS_KEYRING_NAME = os.environ.get("GCP_KMS_KEYRING_NAME", "test-airflow-system-tests-keyring")
GCP_KMS_KEY_NAME = os.environ.get("GCP_KMS_KEY_NAME", "test-airflow-system-tests-key")


@pytest.mark.credential_file(GCP_KMS_KEY)
class TestKmsHookSystem(GoogleSystemTest):
    @provide_gcp_context(GCP_KMS_KEY)
    def test_encrypt(self):
        with TemporaryDirectory() as tmp_dir:
            kms_hook = CloudKMSHook()
            content = kms_hook.encrypt(
                key_name=(
                    f"projects/{kms_hook.project_id}/locations/global/keyRings/"
                    f"{GCP_KMS_KEYRING_NAME}/cryptoKeys/{GCP_KMS_KEY_NAME}"
                ),
                plaintext=b"TEST-SECRET",
            )
            with open(f"{tmp_dir}/mysecret.txt.encrypted", "wb") as encrypted_file:
                encrypted_file.write(base64.b64decode(content))
            self.execute_cmd(
                [
                    "gcloud",
                    "kms",
                    "decrypt",
                    "--location",
                    "global",
                    "--keyring",
                    GCP_KMS_KEYRING_NAME,
                    "--key",
                    GCP_KMS_KEY_NAME,
                    "--ciphertext-file",
                    f"{tmp_dir}/mysecret.txt.encrypted",
                    "--plaintext-file",
                    f"{tmp_dir}/mysecret.txt",
                ]
            )
            with open(f"{tmp_dir}/mysecret.txt", "rb") as secret_file:
                secret = secret_file.read()
            assert secret == b"TEST-SECRET"

    @provide_gcp_context(GCP_KMS_KEY)
    def test_decrypt(self):
        with TemporaryDirectory() as tmp_dir:
            with open(f"{tmp_dir}/mysecret.txt", "w") as secret_file:
                secret_file.write("TEST-SECRET")
            self.execute_cmd(
                [
                    "gcloud",
                    "kms",
                    "encrypt",
                    "--location",
                    "global",
                    "--keyring",
                    GCP_KMS_KEYRING_NAME,
                    "--key",
                    GCP_KMS_KEY_NAME,
                    "--plaintext-file",
                    f"{tmp_dir}/mysecret.txt",
                    "--ciphertext-file",
                    f"{tmp_dir}/mysecret.txt.encrypted",
                ]
            )
            with open(f"{tmp_dir}/mysecret.txt.encrypted", "rb") as encrypted_file:
                encrypted_secret = base64.b64encode(encrypted_file.read()).decode()

            kms_hook = CloudKMSHook()
            content = kms_hook.decrypt(
                key_name=(
                    f"projects/{kms_hook.project_id}/locations/global/keyRings/"
                    f"{GCP_KMS_KEYRING_NAME}/cryptoKeys/{GCP_KMS_KEY_NAME}"
                ),
                ciphertext=encrypted_secret,
            )
            assert content == b"TEST-SECRET"
