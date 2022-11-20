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

import logging

from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import Session

from airflow.models.base import Base
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)


class ImportError(Base):
    """
    A table to store all Import Errors. The ImportErrors are recorded when parsing DAGs.
    This errors are displayed on the Webserver.
    """

    __tablename__ = "import_error"
    id = Column(Integer, primary_key=True)
    timestamp = Column(UtcDateTime)
    filename = Column(String(1024))
    stacktrace = Column(Text)

    @classmethod
    @provide_session
    def purge_filepath(cls, filepath: str, session: Session = NEW_SESSION) -> None:
        """
        Delete ImportError records for given filepath.

        :param filepath: Path of the file for which to remove ImportErrors
        :param session: SQLAlchemy session
        """
        log.debug("Removing ImportErrors where filepath = %s.", filepath)
        session.query(cls).filter(cls.filename == filepath).delete()
