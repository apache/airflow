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
import logging
import os
import struct
from datetime import datetime, timedelta

from sqlalchemy import BigInteger, Column, String, UnicodeText, and_, exists

from airflow.configuration import conf
from airflow.exceptions import AirflowException, DagCodeNotFound
from airflow.models import Base
from airflow.utils import timezone
from airflow.utils.file import correct_maybe_zipped, open_maybe_zipped
from airflow.utils.db import provide_session
from airflow.utils.sqlalchemy import UtcDateTime

log = logging.getLogger(__name__)


class DagCode(Base):
    """A table for DAGs code.

    dag_code table contains code of DAG files synchronized by scheduler.
    This feature is controlled by:

    * ``[core] store_serialized_dags = True``: enable this feature
    * ``[core] store_dag_code = True``: enable this feature

    For details on dag serialization see SerializedDagModel
    """
    __tablename__ = 'dag_code'

    fileloc_hash = Column(
        BigInteger, nullable=False, primary_key=True, autoincrement=False)
    fileloc = Column(String(2000), nullable=False)
    # The max length of fileloc exceeds the limit of indexing.
    last_updated = Column(UtcDateTime, nullable=False)
    source_code = Column(UnicodeText, nullable=False)

    def __init__(self, full_filepath, source_code=None):
        self.fileloc = full_filepath
        self.fileloc_hash = DagCode.dag_fileloc_hash(self.fileloc)
        self.last_updated = timezone.utcnow()
        self.source_code = source_code or DagCode.code(self.fileloc)

    @provide_session
    def sync_to_db(self, session=None):
        """Writes code into database.

        :param session: ORM Session
        """
        self.bulk_sync_to_db([self.fileloc], session)

    @classmethod
    @provide_session
    def bulk_sync_to_db(cls, filelocs, session=None):
        """Writes code in bulk into database.

        :param filelocs: file paths of DAGs to sync
        :param session: ORM Session
        """
        filelocs = set(filelocs)
        filelocs_to_hashes = {
            fileloc: DagCode.dag_fileloc_hash(fileloc) for fileloc in filelocs
        }
        existing_orm_dag_codes = (
            session
            .query(DagCode)
            .filter(DagCode.fileloc_hash.in_(filelocs_to_hashes.values()))
            .with_for_update(of=DagCode)
            .all()
        )

        if existing_orm_dag_codes:
            existing_orm_dag_codes_map = {
                orm_dag_code.fileloc: orm_dag_code for orm_dag_code in existing_orm_dag_codes
            }
        else:
            existing_orm_dag_codes_map = dict()

        existing_orm_dag_codes_by_fileloc_hashes = {
            orm.fileloc_hash: orm for orm in existing_orm_dag_codes
        }
        exisitng_orm_filelocs = {
            orm.fileloc for orm in existing_orm_dag_codes_by_fileloc_hashes.values()
        }
        if not exisitng_orm_filelocs.issubset(filelocs):
            conflicting_filelocs = exisitng_orm_filelocs.difference(filelocs)
            hashes_to_filelocs = {
                DagCode.dag_fileloc_hash(fileloc): fileloc for fileloc in filelocs
            }
            message = ""
            for fileloc in conflicting_filelocs:
                message += ("Filename '{}' causes a hash collision in the " +
                            "database with '{}'. Please rename the file.")\
                    .format(
                        hashes_to_filelocs[DagCode.dag_fileloc_hash(fileloc)],
                        fileloc)
            raise AirflowException(message)

        existing_filelocs = {
            dag_code.fileloc for dag_code in existing_orm_dag_codes
        }
        missing_filelocs = filelocs.difference(existing_filelocs)

        for fileloc in missing_filelocs:
            orm_dag_code = DagCode(fileloc, cls._get_code_from_file(fileloc))
            session.add(orm_dag_code)

        for fileloc in existing_filelocs:
            old_version = existing_orm_dag_codes_by_fileloc_hashes[
                filelocs_to_hashes[fileloc]
            ]
            file_modified = datetime.fromtimestamp(
                os.path.getmtime(correct_maybe_zipped(fileloc)), tz=timezone.utc)

            if (file_modified - timedelta(seconds=120)) > old_version.last_updated:
                orm_dag_code = existing_orm_dag_codes_map[fileloc]
                orm_dag_code.last_updated = timezone.utcnow()
                orm_dag_code.source_code = cls._get_code_from_file(orm_dag_code.fileloc)
                session.merge(orm_dag_code)

    @classmethod
    @provide_session
    def remove_deleted_code(cls, alive_dag_filelocs, session=None):
        """Deletes code not included in alive_dag_filelocs.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param session: ORM Session
        """
        alive_fileloc_hashes = [
            cls.dag_fileloc_hash(fileloc) for fileloc in alive_dag_filelocs]

        log.debug("Deleting code from %s table ", cls.__tablename__)

        session.query(cls).filter(
            and_(cls.fileloc_hash.notin_(alive_fileloc_hashes),
                 cls.fileloc.notin_(alive_dag_filelocs))).delete(synchronize_session='fetch')

    @classmethod
    @provide_session
    def has_dag(cls, fileloc, session=None):
        """Checks a file exist in dag_code table.

        :param fileloc: the file to check
        :param session: ORM Session
        """
        fileloc_hash = cls.dag_fileloc_hash(fileloc)
        return session.query(exists().where(cls.fileloc_hash == fileloc_hash))\
            .scalar()

    @classmethod
    def get_code_by_fileloc(cls, fileloc):
        """Returns source code for a given fileloc.

        :param fileloc: file path of a DAG
        :return: source code as string
        """
        return cls.code(fileloc)

    @classmethod
    def code(cls, fileloc):
        """Returns source code for this DagCode object.

        :return: source code as string
        """
        if conf.getboolean('core', 'store_dag_code', fallback=False):
            return cls._get_code_from_db(fileloc)
        else:
            return cls._get_code_from_file(fileloc)

    @staticmethod
    def _get_code_from_file(fileloc):
        with open_maybe_zipped(fileloc, 'r') as f:
            code = f.read()
        return code

    @classmethod
    @provide_session
    def _get_code_from_db(cls, fileloc, session=None):
        dag_code = session.query(cls) \
            .filter(cls.fileloc_hash == cls.dag_fileloc_hash(fileloc)) \
            .first()
        if not dag_code:
            raise DagCodeNotFound()
        else:
            code = dag_code.source_code
        return code

    @staticmethod
    def dag_fileloc_hash(full_filepath):
        """"Hashing file location for indexing.

        :param full_filepath: full filepath of DAG file
        :return: hashed full_filepath
        """
        # Hashing is needed because the length of fileloc is 2000 as an Airflow convention,
        # which is over the limit of indexing.
        import hashlib
        # Only 7 bytes because MySQL BigInteger can hold only 8 bytes (signed).
        return struct.unpack('>Q', hashlib.sha1(
            full_filepath.encode('utf-8')).digest()[-8:])[0] >> 8
