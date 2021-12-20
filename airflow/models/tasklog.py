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

from sqlalchemy import Column, Integer, Text

from airflow.models.base import Base
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime


class LogFilename(Base):
    """Model to store ``[core] log_filename_template`` config changes.

    This table is automatically populated when Airflow starts up, to store the
    config's value if it does not match the last row in the table.
    """

    __tablename__ = "log_filename"

    id = Column(Integer, primary_key=True, autoincrement=True)
    template = Column(Text, nullable=False)
    created_at = Column(UtcDateTime, nullable=False, default=timezone.utcnow)

    def __repr__(self) -> str:
        created_at = self.created_at.isoformat()
        return f"LogFilename(id={self.id!r}, template={self.template!r}, created_at={created_at!r})"
