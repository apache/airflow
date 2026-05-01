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

from sqlalchemy import ForeignKeyConstraint, Integer, PrimaryKeyConstraint, String, Text
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import COLLATION_ARGS, Base
from airflow.utils.sqlalchemy import UtcDateTime


class AssetStateModel(Base):
    """
    Persists key/value state scoped to an asset identity.

    Not scoped to any DAG run — a watermark written in run 1 is readable by run 2.
    Rows survive until explicitly deleted or the asset itself is deleted.
    """

    __tablename__ = "asset_state"

    asset_id: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    key: Mapped[str] = mapped_column(String(512, **COLLATION_ARGS), nullable=False, primary_key=True)

    value: Mapped[str] = mapped_column(Text().with_variant(MEDIUMTEXT, "mysql"), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("asset_id", "key", name="asset_state_pkey"),
        ForeignKeyConstraint(
            ["asset_id"],
            ["asset.id"],
            name="asset_state_asset_fkey",
            ondelete="CASCADE",
        ),
    )
