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

import uuid6
from sqlalchemy import JSON, Integer, String, Text
from sqlalchemy.orm import Mapped
from sqlalchemy_utils import UUIDType

from airflow._shared.timezones import timezone
from airflow.models import Base
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column


class DeadlineAlert(Base):
    """Table containing DeadlineAlert properties."""

    __tablename__ = "deadline_alert"

    id: Mapped[str] = mapped_column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)

    name: Mapped[str | None] = mapped_column(String(250), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    reference: Mapped[dict] = mapped_column(JSON, nullable=False)
    interval: Mapped[int] = mapped_column(Integer, nullable=False)
    callback: Mapped[dict] = mapped_column(JSON, nullable=False)

    def __repr__(self):
        if self.interval >= 3600:
            interval_display = f"{self.interval // 3600}h"
        elif self.interval >= 60:
            interval_display = f"{self.interval // 60}m"
        else:
            interval_display = f"{self.interval}s"

        return (
            f"[DeadlineAlert] "
            f"id={str(self.id)[:8]}, "
            f"created_at={self.created_at}, "
            f"name={self.name or 'Unnamed'}, "
            f"reference={self.reference}, "
            f"interval={interval_display}, "
            f"callback={self.callback}"
        )
