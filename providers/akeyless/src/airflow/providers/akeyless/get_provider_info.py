# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-akeyless",
        "name": "Akeyless",
        "description": "`Akeyless <https://www.akeyless.io/>`__ Vault Platform provider.\n",
        "integrations": [
            {
                "integration-name": "Akeyless",
                "external-doc-url": "https://docs.akeyless.io/",
                "tags": ["security", "secrets"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Akeyless",
                "python-modules": ["airflow.providers.akeyless.hooks.akeyless"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.akeyless.hooks.akeyless.AkeylessHook",
                "hook-name": "Akeyless",
                "connection-type": "akeyless",
                "ui-field-behaviour": {
                    "hidden-fields": ["extra", "schema", "port"],
                    "relabeling": {
                        "login": "Access ID",
                        "password": "Access Key",
                        "host": "API URL",
                    },
                },
                "conn-fields": {
                    "access_type": {
                        "label": "Access type",
                        "schema": {"type": ["string", "null"]},
                        "description": "One of: api_key, aws_iam, gcp, azure_ad, uid, jwt, k8s, certificate",
                    },
                    "uid_token": {
                        "label": "UID Token",
                        "schema": {"type": ["string", "null"]},
                    },
                    "gcp_audience": {
                        "label": "GCP Audience",
                        "schema": {"type": ["string", "null"]},
                    },
                    "azure_object_id": {
                        "label": "Azure Object ID",
                        "schema": {"type": ["string", "null"]},
                    },
                    "jwt": {
                        "label": "JWT",
                        "schema": {"type": ["string", "null"]},
                    },
                    "k8s_auth_config_name": {
                        "label": "K8s Auth Config Name",
                        "schema": {"type": ["string", "null"]},
                    },
                    "certificate_data": {
                        "label": "Certificate Data (PEM)",
                        "schema": {"type": ["string", "null"]},
                    },
                    "private_key_data": {
                        "label": "Private Key Data (PEM)",
                        "schema": {"type": ["string", "null"]},
                    },
                },
            }
        ],
        "secrets-backends": ["airflow.providers.akeyless.secrets.akeyless.AkeylessBackend"],
    }
