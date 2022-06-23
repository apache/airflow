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
from typing import Dict, Optional
from urllib.parse import urlparse

from sqlalchemy import Column, Index, Integer

from airflow.models.base import Base, StringID
from airflow.utils import timezone
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime


class Dataset(Base):
    """A table to store datasets."""

    id = Column(Integer, primary_key=True, autoincrement=True)
    uri = Column(StringID(length=500), nullable=False)
    extra = Column(ExtendedJSON, nullable=True)
    created_at = Column(UtcDateTime, default=timezone.utcnow(), nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow(), onupdate=timezone.utcnow(), nullable=False)

    __tablename__ = "dataset"
    __table_args__ = (
        Index('idx_uri', uri, unique=True),
        {'sqlite_autoincrement': True},
    )

    def __init__(self, uri, extra: Optional[Dict] = None, **kwargs):
        super().__init__(**kwargs)
        parsed = urlparse(uri)
        if parsed.scheme and parsed.scheme.lower() == 'airflow':
            raise ValueError("Scheme `airflow://` is reserved.")
        self.uri = uri
        self.extra = extra

    def __eq__(self, other):
        return self.uri == other.uri

    def __hash__(self):
        return hash((self.uri, self.extra))
