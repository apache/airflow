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

from typing import TYPE_CHECKING, ClassVar
from urllib.parse import urlparse

from airflow.providers.common.compat.sdk import BaseOperatorLink, BaseSensorOperator, XCom
from airflow.providers.google.version_compat import AIRFLOW_V_3_0_PLUS, BaseOperator

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.providers.common.compat.sdk import Context
    from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

BASE_LINK = "https://console.cloud.google.com"


class BaseGoogleLink(BaseOperatorLink):
    """
    Base class for all Google links.

    When you inherit this class in a Link class;
      - You can call the persist method to push data to the XCom to use it later in the get_link method.
      - If you have an operator which inherit the GoogleCloudBaseOperator or BaseSensorOperator
        You can define extra_links_params method in the operator to pass the operator properties
        to the get_link method.

    :meta private:
    """

    name: ClassVar[str]
    key: ClassVar[str]
    format_str: ClassVar[str]

    @property
    def xcom_key(self) -> str:
        # NOTE: in Airflow 3 we need to have xcom_key property in the Link class.
        # Since we have the key property already, this is just a proxy property method to use same
        # key as in Airflow 2.
        return self.key

    @classmethod
    def persist(cls, context: Context, **value):
        """
        Push arguments to the XCom to use later for link formatting at the `get_link` method.

        Note: for Airflow 2 we need to call this function with context variable only
        where we have the extra_links_params property method defined
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

    def get_config(self, operator, ti_key):
        conf = {}
        conf.update(getattr(operator, "extra_links_params", {}))
        conf.update(XCom.get_value(key=self.key, ti_key=ti_key) or {})

        # if the config did not define, return None to stop URL formatting
        if not conf:
            return None

        # Add a default value for the 'namespace' parameter for backward compatibility.
        # This is for datafusion
        conf.setdefault("namespace", "default")
        return conf

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        if TYPE_CHECKING:
            assert isinstance(operator, (GoogleCloudBaseOperator, BaseSensorOperator))

        # In cases when worker passes execution to trigger, the value that is put to XCom
        # already contains link to the object in string format. In this case we don't want to execute
        # get_config() again. Instead we can leave this value without any changes
        link_value = XCom.get_value(key=self.key, ti_key=ti_key)
        if link_value and isinstance(link_value, str):
            if urlparse(link_value).scheme in ("http", "https"):
                return link_value

        conf = self.get_config(operator, ti_key)
        if not conf:
            return ""
        return self._format_link(**conf)

    def _format_link(self, **kwargs):
        try:
            formatted_str = self.format_str.format(**kwargs)
            if formatted_str.startswith("http"):
                return formatted_str
            return BASE_LINK + formatted_str
        except KeyError:
            return ""
