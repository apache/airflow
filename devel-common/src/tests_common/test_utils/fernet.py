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

from pathlib import Path

import yaml
from cryptography.fernet import Fernet


def generate_fernet_key_string() -> str:
    """Generate a new Fernet key."""
    return Fernet.generate_key().decode()


def update_environment_variable_from_compose_yaml(file_path: str | Path) -> None:
    """Update environment variable AIRFLOW__CORE__FERNET_KEY for a given docker-compose YAML file."""
    file_path = Path(file_path) if isinstance(file_path, str) else file_path
    with file_path.open("r") as f:
        compose_yaml = yaml.safe_load(f)

    x_airflow_common = compose_yaml.get("x-airflow-common", {})
    if x_airflow_common == {}:
        raise ValueError(
            "x-airflow-common was not found in a docker-compose file, please either add it or update here."
        )
    environment = x_airflow_common.get("environment", {})
    environment["AIRFLOW__CORE__FERNET_KEY"] = generate_fernet_key_string()
    x_airflow_common["environment"] = environment
    compose_yaml["x-airflow-common"] = x_airflow_common

    with file_path.open("w") as f:
        yaml.safe_dump(compose_yaml, f)
