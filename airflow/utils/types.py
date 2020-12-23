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
import enum

from sqlalchemy.types import String, TypeDecorator


class EnumString(TypeDecorator):
    """
    Declare db column with this type to make the column compatible with string
    and string based enum values when building the sqlalchemy ORM query. It can
    be used just like sqlalchemy.types.String, for example:

    ```
    class Table(Base):
        __tablename__ = "t"
        run_type = Column(EnumString(50), nullable=False)
    ```
    """

    impl = String

    def process_bind_param(self, value, dialect):
        if isinstance(value, enum.Enum):
            return value.value
        else:
            return value


class DagRunType(enum.Enum):
    """Class with DagRun types"""

    BACKFILL_JOB = "backfill"
    SCHEDULED = "scheduled"
    MANUAL = "manual"

    @staticmethod
    def from_run_id(run_id: str) -> "DagRunType":
        """Resolved DagRun type from run_id."""
        for run_type in DagRunType:
            if run_id and run_id.startswith(f"{run_type.value}__"):
                return run_type
        return DagRunType.MANUAL
