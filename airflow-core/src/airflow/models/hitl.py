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

import base64
import os
from collections.abc import Callable
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Literal
from urllib.parse import urlparse, urlunparse

import attrs
import sqlalchemy_jsonfield
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import Boolean, Column, ForeignKeyConstraint, String, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Session, relationship

from airflow._shared.timezones.timezone import utcnow
from airflow.configuration import conf
from airflow.exceptions import HITLSharedLinkDisabled, HITLSharedLinkTimeout
from airflow.models.base import Base
from airflow.settings import json
from airflow.utils.sqlalchemy import UtcDateTime

HITL_LINK_TYPE = Literal["redirect", "respond"]


def hitl_shared_link_enabled(func: Callable) -> Callable:
    """Check if 'api.hitl_enable_shared_links' is set to True in the config."""

    @wraps(func)
    def wrapper(*args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Any:
        if not conf.getboolean("api", "hitl_enable_shared_links", fallback=False):
            raise HITLSharedLinkDisabled()
        return func(*args, **kwargs)

    return wrapper


@attrs.define
class HITLSharedLinkConfig:
    """Configuration for generating a Human-in-the-loop shared link."""

    link_type: HITL_LINK_TYPE
    expires_at: str
    chosen_options: list[str] = attrs.field(factory=list)
    params_input: dict[str, Any] = attrs.field(factory=dict)

    def __attrs_post_init__(self) -> None:
        if (expires_at_dt := datetime.fromisoformat(self.expires_at)) and expires_at_dt < utcnow():
            raise HITLSharedLinkTimeout()

        if self.link_type == "redirect":
            if self.chosen_options or self.params_input:
                raise ValueError(
                    '"chosen_options" and "params_input" should be empty when "link_type" is set to "redirect"'
                )
        elif self.link_type == "respond":
            if not self.chosen_options:
                raise ValueError('"chosen_options" should not be empty when "link_type" is set to "respond"')
        else:
            raise ValueError(f"Invalid link_type {self.link_type}")

    @staticmethod
    def infer_expires_at(expires_at: datetime | None) -> datetime:
        if expires_at is None:
            hitl_shared_link_expiration_hours = conf.getint(
                "api", "hitl_shared_link_expiration_hours", fallback=24
            )
            expires_at = utcnow() + timedelta(hours=hitl_shared_link_expiration_hours)
        return expires_at


@attrs.define
class HITLSharedLinkData:
    """The data used to generate a Human-in-the-loop shared token."""

    # task instance identifier
    ti_id: str
    dag_id: str
    dag_run_id: str
    task_id: str
    map_index: int | None

    shared_link_config: HITLSharedLinkConfig

    def validate_ti_id(self, session: Session) -> None:
        from airflow.models.taskinstance import TaskInstance

        task_instance = TaskInstance.get_task_instance(
            dag_id=self.dag_id,
            run_id=self.dag_run_id,
            task_id=self.task_id,
            map_index=self.map_index or -1,
            session=session,
        )
        if not task_instance:
            raise ValueError(
                f"The Task Instance with dag_id: `{self.dag_id}`, run_id: `{self.dag_run_id}`,"
                f" task_id: `{self.task_id}` and map_index: `{self.map_index}` was not found"
            )

        if task_instance.id != self.ti_id:
            raise ValueError("Task instance id does not match")

    def encode(self, secret_key: str) -> str:
        """
        Generate a secure token for Human-in-the-loop shared links.

        :return: base64-encoded token
        """
        data = attrs.asdict(self)
        encoded_json = json.dumps(data).encode()
        aesgcm = AESGCM(secret_key.encode())
        iv = os.urandom(12)
        ciphertext = aesgcm.encrypt(iv, encoded_json, None)
        return base64.urlsafe_b64encode(iv + ciphertext).decode("utf-8")

    @staticmethod
    def decode(*, secret_key: str, token: str) -> HITLSharedLinkData:
        """
        Decode a shared link token.

        :param token: Base64-encoded token
        :return: Decoded token data
        """
        raw = base64.urlsafe_b64decode(token.encode())
        if len(raw) < 13:
            raise ValueError("Token too short")

        iv, ciphertext = raw[:12], raw[12:]
        aesgcm = AESGCM(secret_key.encode())
        plaintext = aesgcm.decrypt(iv, ciphertext, None)
        data = json.loads(plaintext)
        return HITLSharedLinkData(
            ti_id=data["ti_id"],
            dag_id=data["dag_id"],
            dag_run_id=data["dag_run_id"],
            task_id=data["task_id"],
            map_index=data["map_index"],
            shared_link_config=HITLSharedLinkConfig(**data["shared_link_config"]),
        )


class HITLDetail(Base):
    """Human-in-the-loop request and corresponding response."""

    __tablename__ = "hitl_detail"
    ti_id = Column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        primary_key=True,
        nullable=False,
    )

    # User Request Detail
    options = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False)
    subject = Column(Text, nullable=False)
    body = Column(Text, nullable=True)
    defaults = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    multiple = Column(Boolean, unique=False, default=False)
    params = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={})

    # Response Content Detail
    response_at = Column(UtcDateTime, nullable=True)
    user_id = Column(String(128), nullable=True)
    chosen_options = Column(
        sqlalchemy_jsonfield.JSONField(json=json),
        nullable=True,
        default=None,
    )
    params_input = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={})
    task_instance = relationship(
        "TaskInstance",
        lazy="joined",
        back_populates="hitl_detail",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            (ti_id,),
            ["task_instance.id"],
            name="hitl_detail_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )

    @hybrid_property
    def response_received(self) -> bool:
        return self.response_at is not None

    @response_received.expression  # type: ignore[no-redef]
    def response_received(cls):
        return cls.response_at.is_not(None)

    @staticmethod
    def _infer_base_url(base_url: str) -> str:
        if not base_url:
            base_url = conf.get("api", "base_url")
            if not base_url:
                raise ValueError("API base_url is not configured")
        return base_url

    @hitl_shared_link_enabled
    def generate_shared_token(
        self,
        secret_key: str,
        # task instance required
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        # shared link required
        link_type: HITL_LINK_TYPE,
        # utility required
        session: Session,
        # task instance optional
        map_index: int | None = None,
        #  shared link optional
        expires_at: datetime | None = None,
        chosen_options: list[str] | None = None,
        params_input: dict[str, Any] | None = None,
    ) -> str:
        """
        Generate a shared link for a Human-in-the-loop detail.

        :param dag_id: Dag ID
        :param dag_run_id: Dag run ID
        :param task_id: Task ID
        :param map_index: Map index for mapped tasks
        :param link_type: Type of link ('redirect' or 'respond')
        :param expires_at: Custom expiration time. (default to 1 day after)
        :param chosen_options: Chosen options for 'respond' links
        :param params_input: Parameters input for 'respond' links
        :param session: Database session

        :return: Link data including URL and metadata
        """
        from airflow.models.taskinstance import TaskInstance

        expires_at = HITLSharedLinkConfig.infer_expires_at(expires_at)

        task_instance = TaskInstance.get_task_instance(
            dag_id=dag_id,
            run_id=dag_run_id,
            task_id=task_id,
            map_index=map_index or -1,
            session=session,
        )
        if task_instance is None:
            raise ValueError("Task instance not found.")

        hitl_shared_link_data = HITLSharedLinkData(
            ti_id=str(task_instance.id),
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            map_index=map_index,
            shared_link_config=HITLSharedLinkConfig(
                link_type=link_type,
                chosen_options=chosen_options or [],
                params_input=params_input or {},
                expires_at=expires_at.isoformat(),
            ),
        )
        return hitl_shared_link_data.encode(secret_key=secret_key)

    @hitl_shared_link_enabled
    def generate_shared_link(
        self,
        *,
        secret_key: str,
        # task instance required
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        # shared link required
        link_type: HITL_LINK_TYPE,
        base_url: str,
        # utility required
        session: Session,
        # task instance optional
        map_index: int | None = None,
        #  shared link optional
        expires_at: datetime | None = None,
        chosen_options: list[str] | None = None,
        params_input: dict[str, Any] | None = None,
    ) -> str:
        if chosen_options and (self.multiple is False and len(chosen_options) > 1):
            raise ValueError(
                f"Multiple chosen options '{chosen_options}' is not allowed when multiple is set to False"
            )

        token = self.generate_shared_token(
            # task instance identifier
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            map_index=map_index,
            # Human-in-the-loop shared link config
            link_type=link_type,
            expires_at=expires_at,
            chosen_options=chosen_options,
            params_input=params_input,
            # utility
            secret_key=secret_key,
            session=session,
        )

        base_url = HITLDetail._infer_base_url(base_url)
        parsed_base_url = urlparse(base_url)
        return urlunparse(
            (
                parsed_base_url.scheme,
                parsed_base_url.netloc,
                f"/api/v2/hitlSharedLinks/{link_type}/{token}",
                "",
                "",
                "",
            ),
        )

    @hitl_shared_link_enabled
    @staticmethod
    def from_shared_token_to_redirect_url(
        *,
        token: str,
        secret_key: str,
        base_url: str,
        session: Session,
    ) -> str:
        """
        Retrieve the redirect URL to Airflow UI through token.

        :param token: Human-in-the-loop Shared link token
        :param secret_key: Secret key used to decode the token
        :param base_url: Base URL for Airflow instance
        :param session: Database session

        :return: Redirect URL to Airflow UI
        """
        shared_data = HITLSharedLinkData.decode(token=token, secret_key=secret_key)
        shared_link_config = shared_data.shared_link_config
        if (link_type := shared_link_config.link_type) != "redirect":
            raise ValueError(f"Unexpected link_type '{link_type}'")

        shared_data.validate_ti_id(session=session)

        url_ti_part = f"/dags/{shared_data.dag_id}/runs/{shared_data.dag_run_id}/tasks/{shared_data.task_id}"
        if shared_data.map_index is not None:
            url_ti_part = f"{url_ti_part}/{shared_data.map_index}"

        parsed_base_url = urlparse(base_url)
        return urlunparse(
            (
                parsed_base_url.scheme,
                parsed_base_url.netloc,
                f"{url_ti_part}/required_actions",
                "",
                "",
                "",
            ),
        )
