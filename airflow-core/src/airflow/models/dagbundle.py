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

from sqlalchemy import Boolean, Column, String
from sqlalchemy.orm import relationship
from sqlalchemy_utils import JSONType

from airflow.models.base import Base, StringID
from airflow.models.team import dag_bundle_team_association_table
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import UtcDateTime


class DagBundleModel(Base, LoggingMixin):
    """
    A table for storing DAG bundle metadata.

    We track the following information about each bundle, as it can be useful for
    informational purposes and for debugging:

    - active: Is the bundle currently found in configuration?
    - version: The latest version Airflow has seen for the bundle.
    - last_refreshed: When the bundle was last refreshed.
    - signed_url_template: Signed URL template for viewing the bundle
    - template_params: JSON object containing template parameters for constructing view url (e.g., {"subdir": "dags"})

    """

    __tablename__ = "dag_bundle"
    name = Column(StringID(), primary_key=True, nullable=False)
    active = Column(Boolean, default=True)
    version = Column(String(200), nullable=True)
    last_refreshed = Column(UtcDateTime, nullable=True)
    signed_url_template = Column(String(200), nullable=True)
    template_params = Column(JSONType, nullable=True)
    teams = relationship("Team", secondary=dag_bundle_team_association_table, back_populates="dag_bundles")

    def __init__(self, *, name: str, version: str | None = None):
        super().__init__()
        self.name = name
        self.version = version

    def _unsign_url(self) -> str | None:
        """
        Unsign a URL token to get the original URL template.

        :param signed_url: The signed URL token
        :return: The original URL template or None if unsigning fails
        """
        try:
            from itsdangerous import BadSignature, URLSafeSerializer

            from airflow.configuration import conf

            serializer = URLSafeSerializer(conf.get_mandatory_value("core", "fernet_key"))
            payload = serializer.loads(self.signed_url_template)
            if isinstance(payload, dict) and "url" in payload and "bundle_name" in payload:
                if payload["bundle_name"] == self.name:
                    return payload["url"]

            return None
        except (BadSignature, Exception):
            return None

    def render_url(self, version: str | None = None) -> str | None:
        """
        Render the URL template with the given version and stored template parameters.

        First unsigns the URL to get the original template, then formats it with
        the provided version and any additional parameters.

        :param version: The version to substitute in the template
        :return: The rendered URL or None if no template is available
        """
        if not self.signed_url_template:
            return None

        url_template = self._unsign_url()

        if url_template is None:
            return None

        params = dict(self.template_params or {})
        params["version"] = version

        try:
            return url_template.format(**params)
        except (KeyError, ValueError) as e:
            self.log.warning("Failed to render URL template for bundle %s: %s", self.name, e)
            return None
