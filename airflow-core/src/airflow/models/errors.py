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

from datetime import datetime

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column


class ParseImportError(Base):
    """Stores all Import Errors which are recorded when parsing DAGs and displayed on the Webserver."""

    __tablename__ = "import_error"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    filename: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    bundle_name: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    stacktrace: Mapped[str | None] = mapped_column(Text, nullable=True)

    def full_file_path(self) -> str:
        """Return the full file path of the dag."""
        if self.bundle_name is None or self.filename is None:
            raise ValueError("bundle_name and filename must not be None")
        bundle = DagBundlesManager().get_bundle(self.bundle_name)
        return "/".join([str(bundle.path), self.filename])
