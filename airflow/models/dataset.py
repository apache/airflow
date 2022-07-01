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
from urllib.parse import urlparse

from sqlalchemy import Column, Index, Integer, String

from airflow.models.base import Base
from airflow.models.dataset_dag_ref import DatasetDagRef
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime


class Dataset(Base):
    """
    A table to store datasets.

    :param uri: a string that uniquely identifies the dataset
    :param extra: JSON field for arbitrary extra info
    """

    id = Column(Integer, primary_key=True, autoincrement=True)
    uri = Column(
        String(length=3000).with_variant(
            String(
                length=3000,
                # latin1 allows for more indexed length in mysql
                # and this field should only be ascii chars
                collation='latin1_general_cs',
            ),
            'mysql',
        ),
        nullable=False,
    )
    extra = Column(ExtendedJSON, nullable=True)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    __tablename__ = "dataset"
    __table_args__ = (
        Index('idx_uri_unique', uri, unique=True),
        {'sqlite_autoincrement': True},  # ensures PK values not reused
    )

    def __init__(self, uri: str, **kwargs):
        try:
            uri.encode('ascii')
        except UnicodeEncodeError:
            raise ValueError('URI must be ascii')
        parsed = urlparse(uri)
        if parsed.scheme and parsed.scheme.lower() == 'airflow':
            raise ValueError("Scheme `airflow` is reserved.")
        super().__init__(uri=uri, **kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.uri == other.uri
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.uri)

    def __repr__(self):
        return f"{self.__class__.__name__}(uri={self.uri!r}, extra={self.extra!r})"

    @provide_session
    def get_downstream_dag_ids(self, session=NEW_SESSION):
        if not self.id:
            raise RuntimeError("Dataset must be persisted to database before we can look up downstream dags.")
        return [
            x.dag_id
            for x in session.query(DatasetDagRef.dag_id)
            .filter(
                DatasetDagRef.dataset_id == self.id,
            )
            .all()
        ]

    @provide_session
    def get_dataset_id(self, session=NEW_SESSION):
        if self.id:
            dataset_id = self.id
        else:
            stored = session.query(self.__class__).filter(self.__class__.uri == self.uri).first()
            if not stored:
                session.add(self)
                session.flush()
                dataset_id = self.id
            else:
                dataset_id = stored.id
        return dataset_id
