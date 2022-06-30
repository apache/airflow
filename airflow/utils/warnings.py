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
"""Various user-facing warnings"""


def warn_list_secrets_alternative_backend(
    cli_or_ui: str,
    connection_or_variable: str,
) -> str:
    """Warning for if a user is trying to list secrets that are stored in an alternative backend"""
    if cli_or_ui not in ('cli', 'ui'):
        raise ValueError(f"Received {cli_or_ui} for `cli_or_ui`. Must be either 'cli' or 'ui'.")
    if connection_or_variable not in ('connection', 'variable'):
        raise ValueError(
            f"Received {connection_or_variable} for `connection_or_variable`. "
            "Must be either 'connection' or 'variable'."
        )
    return (
        f"The Airflow {cli_or_ui.upper()} will not return {connection_or_variable.title()}s "
        "stored in an alternative secrets backend such as BaseSecretsBackend or "
        "EnvironmentVariablesBackend."
    )
