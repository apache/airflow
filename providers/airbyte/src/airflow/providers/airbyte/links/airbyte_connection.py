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

from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlparse

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.airbyte.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.providers.common.compat.sdk import BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
    from airflow.providers.airbyte.version_compat import BaseOperator
    from airflow.providers.common.compat.sdk import Context, TaskInstanceKey


AIRBYTE_CLOUD_BASE_LINK = "https://cloud.airbyte.com"
AIRBYTE_WORKSPACE_LINK = "/workspaces/{workspace_id}"
AIRBYTE_CONNECTIONS_LINK = "/connections/{connection_id}"


class AirbyteConnectionLink(BaseOperatorLink):
    """
    Constructs a link to the Airbyte connection details page.

    :meta private:
    """

    key: ClassVar[str] = "airbyte_connection"

    @property
    def name(self):
        return "Airbyte Connection"

    @property
    def xcom_key(self):
        return self.key

    @classmethod
    def persist(cls, context: Context, **value):
        """
        Push arguments to the XCom to use later for link formatting at the `get_link` method.

        Note: for Airflow 2 we need to call this function with context variable only
        where we have the extra_links_params property method defined.

        Copied from `airflow.providers.google.cloud.links.BaseGoogleLink`.
        """
        params = {}
        # TODO: remove after Airflow v2 support dropped
        if not AIRFLOW_V_3_0_PLUS:
            common_params = getattr(context["task"], "extra_links_params", None)
            if common_params:
                params.update(common_params)

        context["ti"].xcom_push(
            key=cls.key,
            value={
                **params,
                **value,
            },
        )

    def _get_config(self, link_value: dict[str, Any]):
        conf = {}

        server_url = link_value.pop("server_url")

        parsed_url = urlparse(server_url)
        host = parsed_url.hostname

        if host and host.endswith(".airbyte.com"):
            target = AIRBYTE_CLOUD_BASE_LINK

        else:
            target = parsed_url._replace(path="", fragment="").geturl()

        conf.update(link_value)
        conf["target"] = target

        return conf

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        if TYPE_CHECKING:
            assert isinstance(operator, (AirbyteTriggerSyncOperator))

        link_value = XCom.get_value(key=self.key, ti_key=ti_key)
        conf = self._get_config(link_value)

        return self._format_link(**conf)

    def _format_link(self, target, workspace_id, connection_id):

        if workspace_id:
            template_str = "{target}" + AIRBYTE_WORKSPACE_LINK + AIRBYTE_CONNECTIONS_LINK

        else:
            template_str = "{target}" + AIRBYTE_CONNECTIONS_LINK

        return template_str.format(connection_id=connection_id, target=target, workspace_id=workspace_id)


class AirbyteConnectionLinkPlugin(AirflowPlugin):
    """
    Plugin to register AirbyteConnectionLink.

    :meta private:
    """

    name = "airbyte_connection_link_plugin"
    operator_extra_links = [AirbyteConnectionLink()]
