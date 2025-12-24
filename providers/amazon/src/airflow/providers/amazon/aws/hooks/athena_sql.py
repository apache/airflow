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

import json
from functools import cached_property
from typing import TYPE_CHECKING, Any

import pyathena
from sqlalchemy.engine.url import URL

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper
from airflow.providers.common.compat.sdk import AirflowException, AirflowNotFoundException
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from pyathena.connection import Connection as AthenaConnection


class AthenaSQLHook(AwsBaseHook, DbApiHook):
    """
    Interact with Amazon Athena.

    Provide wrapper around PyAthena library.

    :param athena_conn_id: :ref:`Amazon Athena Connection <howto/connection:athena>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    You can specify ``driver`` in ``extra`` of your connection in order to use
    a different driver than the default ``rest``.

    Also, aws_domain could be specified in ``extra`` of your connection.

    PyAthena and AWS Authentication parameters could be passed in extra field of ``athena_conn_id`` connection.

    Passing authentication parameters in ``athena_conn_id`` will override those in ``aws_conn_id``.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    .. note::
        get_uri() depends on SQLAlchemy and PyAthena
    """

    conn_name_attr = "athena_conn_id"
    default_conn_name = "athena_default"
    conn_type = "athena"
    hook_name = "Amazon Athena"
    supports_autocommit = True

    def __init__(self, athena_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.athena_conn_id = athena_conn_id

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for AWS Athena Connection."""
        return {
            "hidden_fields": ["host", "port"],
            "relabeling": {
                "login": "AWS Access Key ID",
                "password": "AWS Secret Access Key",
            },
            "placeholders": {
                "login": "AKIAIOSFODNN7EXAMPLE",
                "password": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "extra": json.dumps(
                    {
                        "aws_domain": "amazonaws.com",
                        "driver": "rest",
                        "s3_staging_dir": "s3://bucket_name/staging/",
                        "work_group": "primary",
                        "region_name": "us-east-1",
                        "session_kwargs": {"profile_name": "default"},
                        "config_kwargs": {"retries": {"mode": "standard", "max_attempts": 10}},
                        "role_arn": "arn:aws:iam::123456789098:role/role-name",
                        "assume_role_method": "assume_role",
                        "assume_role_kwargs": {"RoleSessionName": "airflow"},
                        "aws_session_token": "AQoDYXdzEJr...EXAMPLETOKEN",
                        "endpoint_url": "http://localhost:4566",
                    },
                    indent=2,
                ),
            },
        }

    @cached_property
    def conn_config(self) -> AwsConnectionWrapper:
        """Get the Airflow Connection object and wrap it in helper (cached)."""
        athena_conn = self.get_connection(self.athena_conn_id)
        if self.aws_conn_id:
            try:
                connection = self.get_connection(self.aws_conn_id)
                connection.login = athena_conn.login
                connection.password = athena_conn.password
                connection.schema = athena_conn.schema
                merged_extra = {**athena_conn.extra_dejson, **connection.extra_dejson}
                try:
                    extra_json = json.dumps(merged_extra)
                    connection.extra = extra_json
                except (TypeError, ValueError):
                    raise ValueError(
                        f"Encountered non-JSON in `extra` field for connection {self.aws_conn_id!r}."
                    )
            except AirflowNotFoundException:
                connection = athena_conn
                connection.conn_type = "aws"
                self.log.warning(
                    "Unable to find AWS Connection ID '%s', switching to empty.", self.aws_conn_id
                )

        return AwsConnectionWrapper(
            conn=connection,
            region_name=self._region_name,
            botocore_config=self._config,
            verify=self._verify,
        )

    @property
    def conn(self) -> AwsConnectionWrapper:
        """Get Aws Connection Wrapper object."""
        return self.conn_config

    def _get_conn_params(self) -> dict[str, str | None]:
        """Retrieve connection parameters."""
        if not self.conn.region_name:
            raise AirflowException("region_name must be specified in the connection's extra")

        return dict(
            driver=self.conn.extra_dejson.get("driver", "rest"),
            schema_name=self.conn.schema,
            region_name=self.conn.region_name,
            aws_domain=self.conn.extra_dejson.get("aws_domain", "amazonaws.com"),
        )

    def get_uri(self) -> str:
        """Overridden to use the Athena dialect as driver name."""
        conn_params = self._get_conn_params()
        creds = self.get_credentials(region_name=conn_params["region_name"])

        return URL.create(
            f"awsathena+{conn_params['driver']}",
            username=creds.access_key,
            password=creds.secret_key,
            host=f"athena.{conn_params['region_name']}.{conn_params['aws_domain']}",
            port=443,
            database=conn_params["schema_name"],
            query={"aws_session_token": creds.token, **self.conn.extra_dejson},
        ).render_as_string(hide_password=False)

    def get_conn(self) -> AthenaConnection:
        """Get a ``pyathena.Connection`` object."""
        conn_params = self._get_conn_params()

        conn_kwargs: dict = {
            "schema_name": conn_params["schema_name"],
            "region_name": conn_params["region_name"],
            "session": self.get_session(region_name=conn_params["region_name"]),
            **self.conn.extra_dejson,
        }

        return pyathena.connect(**conn_kwargs)
