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

import warnings

from sqlalchemy import Column, Integer, String, Text

from airflow.exceptions import RemovedInAirflow3Warning
from airflow.models.base import Base
from airflow.utils.sqlalchemy import UtcDateTime


class ParseImportError(Base):
    """Stores all Import Errors which are recorded when parsing DAGs and displayed on the Webserver."""

    __tablename__ = "import_error"
    id = Column(Integer, primary_key=True)
    timestamp = Column(UtcDateTime)
    filename = Column(String(1024))
    stacktrace = Column(Text)
    processor_subdir = Column(String(2000), nullable=True)


def __getattr__(name: str):
    # PEP-562: Lazy loaded attributes on python modules
    if name == "ImportError":
        warnings.warn(
            f"Model class '{__name__}.ImportError' is deprecated due to shadowing with builtin exception "
            f"ImportError and will be removed in the future. "
            f"Please consider to use '{__name__}.ParseImportError' instead.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return ParseImportError
    raise AttributeError(f"module {__name__} has no attribute {name}")
