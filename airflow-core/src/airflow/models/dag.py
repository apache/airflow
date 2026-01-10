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
from collections import defaultdict
from collections.abc import Callable, Collection
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import pendulum
import sqlalchemy_jsonfield
from sqlalchemy import (
    Boolean,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    and_,
    case,
    func,
    or_,
    select,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, Session, backref, joinedload, load_only, relationship
from sqlalchemy.sql import expression

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.assets.evaluation import AssetEvaluator
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException
from airflow.models.asset import AssetDagRunQueue, AssetModel
from airflow.models.base import Base, StringID
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.team import Team
from airflow.sdk.definitions.deadline import DeadlineAlert
from airflow.serialization.definitions.assets import SerializedAssetUniqueKey
from airflow.settings import json
from airflow.timetables.base import DataInterval, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
from airflow.timetables.simple import AssetTriggeredTimetable, NullTimetable, OnceTimetable
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column, with_row_locks
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from typing import TypeAlias

    from dateutil.relativedelta import relativedelta

    from airflow.sdk import Context
    from airflow.serialization.definitions.assets import (
        SerializedAsset,
        SerializedAssetAlias,
        SerializedAssetBase,
    )
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.serialized_objects import LazyDeserializedDAG

    UKey: TypeAlias = SerializedAssetUniqueKey
    DagStateChangeCallback = Callable[[Context], None]
    ScheduleInterval = None | str | timedelta | relativedelta

    ScheduleArg = (
        ScheduleInterval
        | Timetable
        | "SerializedAssetBase"
        | Collection["SerializedAsset" | "SerializedAssetAlias"]
    )

log = logging.getLogger(__name__)

TAG_MAX_LEN = 100


def infer_automated_data_interval(timetable: Timetable, logical_date: datetime) -> DataInterval:
    """
    Infer a data interval for a run against this DAG.

    This method is used to bridge runs created prior to AIP-39
    implementation, which do not have an explicit data interval. Therefore,
    this method only considers ``schedule_interval`` values valid prior to
    Airflow 2.2.

    DO NOT call this method if there is a known data interval.

    :meta private:
    """
    timetable_type = type(timetable)
    if issubclass(timetable_type, (NullTimetable, OnceTimetable, AssetTriggeredTimetable)):
        return DataInterval.exact(timezone.coerce_datetime(logical_date))
    start = timezone.coerce_datetime(logical_date)
    if issubclass(timetable_type, CronDataIntervalTimetable):
        end = cast("CronDataIntervalTimetable", timetable)._get_next(start)
    elif issubclass(timetable_type, DeltaDataIntervalTimetable):
        end = cast("DeltaDataIntervalTimetable", timetable)._get_next(start)
    # Contributors: When the exception below is raised, you might want to
    # add an 'elif' block here to handle custom timetables. Stop! The bug
    # you're looking for is instead at when the DAG run (represented by
    # logical_date) was created. See GH-31969 for an example:
    # * Wrong fix: GH-32074 (modifies this function).
    # * Correct fix: GH-32118 (modifies the DAG run creation code).
    else:
        raise ValueError(f"Not a valid timetable: {timetable!r}")
    return DataInterval(start, end)


def get_run_data_interval(timetable: Timetable, run: DagRun) -> DataInterval:
    """
    Get the data interval of this run.

    For compatibility, this method infers the data interval from the DAG's
    schedule if the run does not have an explicit one set, which is possible for
    runs created prior to AIP-39.

    This function is private to Airflow core and should not be depended on as a
    part of the Python API.

    :meta private:
    """
    if (
        data_interval := _get_model_data_interval(run, "data_interval_start", "data_interval_end")
    ) is not None:
        return data_interval

    if (
        data_interval := timetable.infer_manual_data_interval(run_after=pendulum.instance(run.run_after))
    ) is not None:
        return data_interval

    # Compatibility: runs created before AIP-39 implementation don't have an
    # explicit data interval. Try to infer from the logical date.
    if TYPE_CHECKING:
        assert run.logical_date is not None
    return infer_automated_data_interval(timetable, run.logical_date)


def get_next_data_interval(timetable: Timetable, dag_model: DagModel) -> DataInterval | None:
    """
    Get the data interval of the next scheduled run.

    For compatibility, this method infers the data interval from the DAG's
    schedule if the run does not have an explicit one set, which is possible
    for runs created prior to AIP-39.

    This function is private to Airflow core and should not be depended on as a
    part of the Python API.

    :meta private:
    """
    if dag_model.next_dagrun is None:  # Next run not scheduled.
        return None
    data_interval = dag_model.next_dagrun_data_interval
    if data_interval is not None:
        return data_interval

    # Compatibility: A run was scheduled without an explicit data interval.
    # This means the run was scheduled before AIP-39 implementation. Try to
    # infer from the logical date.
    return infer_automated_data_interval(timetable, dag_model.next_dagrun)


class InconsistentDataInterval(AirflowException):
    """
    Exception raised when a model populates data interval fields incorrectly.

    The data interval fields should either both be None (for runs scheduled
    prior to AIP-39), or both be datetime (for runs scheduled after AIP-39 is
    implemented). This is raised if exactly one of the fields is None.
    """

    _template = (
        "Inconsistent {cls}: {start[0]}={start[1]!r}, {end[0]}={end[1]!r}, "
        "they must be either both None or both datetime"
    )

    def __init__(self, instance: Any, start_field_name: str, end_field_name: str) -> None:
        self._class_name = type(instance).__name__
        self._start_field = (start_field_name, getattr(instance, start_field_name))
        self._end_field = (end_field_name, getattr(instance, end_field_name))

    def __str__(self) -> str:
        return self._template.format(cls=self._class_name, start=self._start_field, end=self._end_field)


def _get_model_data_interval(
    instance: Any,
    start_field_name: str,
    end_field_name: str,
) -> DataInterval | None:
    start = timezone.coerce_datetime(getattr(instance, start_field_name))
    end = timezone.coerce_datetime(getattr(instance, end_field_name))
    if start is None:
        if end is not None:
            raise InconsistentDataInterval(instance, start_field_name, end_field_name)
        return None
    if end is None:
        raise InconsistentDataInterval(instance, start_field_name, end_field_name)
    return DataInterval(start, end)


def get_last_dagrun(dag_id: str, session: Session, include_manually_triggered: bool = False) -> DagRun | None:
    """
    Return the last dag run for a dag, None if there was none.

    Last dag run can be any type of run e.g. scheduled or backfilled.
    Overridden DagRuns are ignored.
    """
    DR = DagRun
    query = select(DR).where(DR.dag_id == dag_id, DR.logical_date.is_not(None))
    if not include_manually_triggered:
        query = query.where(DR.run_type != DagRunType.MANUAL)
    query = query.order_by(DR.logical_date.desc())
    return session.scalar(query.limit(1))


def get_asset_triggered_next_run_info(
    dag_ids: list[str], *, session: Session
) -> dict[str, dict[str, int | str]]:
    """
    Get next run info for a list of dag_ids.

    Given a list of dag_ids, get string representing how close any that are asset triggered are
    their next run, e.g. "1 of 2 assets updated".
    """
    from airflow.models.asset import AssetDagRunQueue as ADRQ, DagScheduleAssetReference

    return {
        x.dag_id: {
            "uri": x.uri,
            "ready": x.ready,
            "total": x.total,
        }
        for x in session.execute(
            select(
                DagScheduleAssetReference.dag_id,
                # This is a dirty hack to workaround group by requiring an aggregate,
                # since grouping by asset is not what we want to do here...but it works
                case((func.count() == 1, func.max(AssetModel.uri)), else_="").label("uri"),
                func.count().label("total"),
                func.sum(case((ADRQ.target_dag_id.is_not(None), 1), else_=0)).label("ready"),
            )
            .join(
                ADRQ,
                and_(
                    ADRQ.asset_id == DagScheduleAssetReference.asset_id,
                    ADRQ.target_dag_id == DagScheduleAssetReference.dag_id,
                ),
                isouter=True,
            )
            .join(AssetModel, AssetModel.id == DagScheduleAssetReference.asset_id)
            .group_by(DagScheduleAssetReference.dag_id)
            .where(DagScheduleAssetReference.dag_id.in_(dag_ids))
        ).all()
    }


class DagTag(Base):
    """A tag name per dag, to allow quick filtering in the DAG view."""

    __tablename__ = "dag_tag"
    name: Mapped[str] = mapped_column(String(TAG_MAX_LEN), primary_key=True)
    dag_id: Mapped[str] = mapped_column(
        StringID(),
        ForeignKey("dag.dag_id", name="dag_tag_dag_id_fkey", ondelete="CASCADE"),
        primary_key=True,
    )

    __table_args__ = (Index("idx_dag_tag_dag_id", dag_id),)

    def __repr__(self):
        return self.name


class DagOwnerAttributes(Base):
    """
    Table defining different owner attributes.

    For example, a link for an owner that will be passed as a hyperlink to the "DAGs" view.
    """

    __tablename__ = "dag_owner_attributes"
    dag_id: Mapped[str] = mapped_column(
        StringID(),
        ForeignKey("dag.dag_id", name="dag.dag_id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    owner: Mapped[str] = mapped_column(String(500), primary_key=True, nullable=False)
    link: Mapped[str] = mapped_column(String(500), nullable=False)

    def __repr__(self):
        return f"<DagOwnerAttributes: dag_id={self.dag_id}, owner={self.owner}, link={self.link}>"

    @classmethod
    def get_all(cls, session: Session) -> dict[str, dict[str, str]]:
        dag_links: dict = defaultdict(dict)
        for obj in session.scalars(select(cls)):
            dag_links[obj.dag_id].update({obj.owner: obj.link})
        return dag_links


class DagModel(Base):
    """Table containing DAG properties."""

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information.
    """
    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = airflow_conf.getboolean("core", "dags_are_paused_at_creation")
    is_paused: Mapped[bool] = mapped_column(Boolean, default=is_paused_at_creation)
    # Whether that DAG was seen on the last DagBag load
    is_stale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    exceeds_max_non_backfill: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Last time the scheduler started
    last_parsed_time: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    # How long it took to parse this file
    last_parse_duration: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Time when the DAG last received a refresh signal
    # (e.g. the DAG's "refresh" button was clicked in the web UI)
    last_expired: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    # The location of the file containing the DAG object
    # Note: Do not depend on fileloc pointing to a file; in the case of a
    # packaged DAG, it will point to the subpath of the DAG within the
    # associated zip.
    fileloc: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    relative_fileloc: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    bundle_name: Mapped[str] = mapped_column(StringID(), ForeignKey("dag_bundle.name"), nullable=False)
    # The version of the bundle the last time the DAG was processed
    bundle_version: Mapped[str | None] = mapped_column(String(200), nullable=True)
    # String representing the owners
    owners: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    # Display name of the dag
    _dag_display_property_value: Mapped[str | None] = mapped_column(
        "dag_display_name", String(2000), nullable=True
    )
    # Description of the dag
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Timetable summary
    timetable_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Timetable description
    timetable_description: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    # Asset expression based on asset triggers
    asset_expression: Mapped[dict[str, Any] | None] = mapped_column(
        sqlalchemy_jsonfield.JSONField(json=json), nullable=True
    )
    # DAG deadline information
    _deadline: Mapped[dict[str, Any] | None] = mapped_column(
        "deadline", sqlalchemy_jsonfield.JSONField(json=json), nullable=True
    )
    # Tags for view filter
    tags = relationship("DagTag", cascade="all, delete, delete-orphan", backref=backref("dag"))
    # Dag owner links for DAGs view
    dag_owner_links = relationship(
        "DagOwnerAttributes", cascade="all, delete, delete-orphan", backref=backref("dag")
    )

    max_active_tasks: Mapped[int] = mapped_column(Integer, nullable=False)
    max_active_runs: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )  # todo: should not be nullable if we have a default
    max_consecutive_failed_dag_runs: Mapped[int] = mapped_column(Integer, nullable=False)

    has_task_concurrency_limits: Mapped[bool] = mapped_column(Boolean, nullable=False)
    has_import_errors: Mapped[bool] = mapped_column(Boolean(), default=False, server_default="0")
    fail_fast: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")

    # The logical date of the next dag run.
    next_dagrun: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    # Must be either both NULL or both datetime.
    next_dagrun_data_interval_start: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    next_dagrun_data_interval_end: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    # Earliest time at which this ``next_dagrun`` can be created.
    next_dagrun_create_after: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    __table_args__ = (Index("idx_next_dagrun_create_after", next_dagrun_create_after, unique=False),)

    schedule_asset_references = relationship(
        "DagScheduleAssetReference",
        back_populates="dag",
        cascade="all, delete, delete-orphan",
    )
    schedule_asset_alias_references = relationship(
        "DagScheduleAssetAliasReference",
        back_populates="dag",
        cascade="all, delete, delete-orphan",
    )
    schedule_asset_name_references = relationship(
        "DagScheduleAssetNameReference",
        back_populates="dag",
        cascade="all, delete, delete-orphan",
    )
    schedule_asset_uri_references = relationship(
        "DagScheduleAssetUriReference",
        back_populates="dag",
        cascade="all, delete, delete-orphan",
    )
    schedule_assets = association_proxy("schedule_asset_references", "asset")
    task_inlet_asset_references = relationship(
        "TaskInletAssetReference",
        cascade="all, delete, delete-orphan",
    )
    task_outlet_asset_references = relationship(
        "TaskOutletAssetReference",
        cascade="all, delete, delete-orphan",
    )
    NUM_DAGS_PER_DAGRUN_QUERY = airflow_conf.getint(
        "scheduler", "max_dagruns_to_create_per_loop", fallback=10
    )
    dag_versions = relationship(
        "DagVersion", back_populates="dag_model", cascade="all, delete, delete-orphan"
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.max_active_tasks is None:
            self.max_active_tasks = airflow_conf.getint("core", "max_active_tasks_per_dag")

        if self.max_active_runs is None:
            self.max_active_runs = airflow_conf.getint("core", "max_active_runs_per_dag")

        if self.max_consecutive_failed_dag_runs is None:
            self.max_consecutive_failed_dag_runs = airflow_conf.getint(
                "core", "max_consecutive_failed_dag_runs_per_dag"
            )

        if self.has_task_concurrency_limits is None:
            # Be safe -- this will be updated later once the DAG is parsed
            self.has_task_concurrency_limits = True

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    @property
    def next_dagrun_data_interval(self) -> DataInterval | None:
        return _get_model_data_interval(
            self,
            "next_dagrun_data_interval_start",
            "next_dagrun_data_interval_end",
        )

    @next_dagrun_data_interval.setter
    def next_dagrun_data_interval(self, value: tuple[datetime, datetime] | None) -> None:
        if value is None:
            self.next_dagrun_data_interval_start = self.next_dagrun_data_interval_end = None
        else:
            self.next_dagrun_data_interval_start, self.next_dagrun_data_interval_end = value

    @property
    def deadline(self):
        """Get the deserialized deadline alert."""
        if self._deadline is None:
            return None
        if isinstance(self._deadline, list):
            return [DeadlineAlert.deserialize_deadline_alert(item) for item in self._deadline]
        return DeadlineAlert.deserialize_deadline_alert(self._deadline)

    @deadline.setter
    def deadline(self, value):
        """Set and serialize the deadline alert."""
        if value is None:
            self._deadline = None
        elif isinstance(value, list):
            self._deadline = [
                item if isinstance(item, dict) else item.serialize_deadline_alert() for item in value
            ]
        elif isinstance(value, dict):
            self._deadline = value
        else:
            self._deadline = value.serialize_deadline_alert()

    @property
    def timezone(self):
        return settings.TIMEZONE

    @staticmethod
    @provide_session
    def get_dagmodel(dag_id: str, session: Session = NEW_SESSION) -> DagModel | None:
        return session.get(
            DagModel,
            dag_id,
        )

    @classmethod
    @provide_session
    def get_current(cls, dag_id: str, session: Session = NEW_SESSION) -> DagModel | None:
        return session.scalar(select(cls).where(cls.dag_id == dag_id))

    @provide_session
    def get_last_dagrun(
        self, session: Session = NEW_SESSION, include_manually_triggered: bool = False
    ) -> DagRun | None:
        return get_last_dagrun(
            self.dag_id, session=session, include_manually_triggered=include_manually_triggered
        )

    def get_is_active(self, *, session: Session | None = None) -> bool:
        """Provide interface compatibility to 'DAG'."""
        return not self.is_stale

    @staticmethod
    @provide_session
    def get_paused_dag_ids(dag_ids: list[str], session: Session = NEW_SESSION) -> set[str]:
        """
        Given a list of dag_ids, get a set of Paused Dag Ids.

        :param dag_ids: List of Dag ids
        :param session: ORM Session
        :return: Paused Dag_ids
        """
        paused_dag_ids = session.scalars(
            select(DagModel.dag_id)
            .where(DagModel.is_paused == expression.true())
            .where(DagModel.dag_id.in_(dag_ids))
        )

        return set(paused_dag_ids)

    @property
    def safe_dag_id(self):
        return self.dag_id.replace(".", "__dot__")

    @hybrid_property
    def dag_display_name(self) -> str:
        return self._dag_display_property_value or self.dag_id

    @dag_display_name.expression  # type: ignore[no-redef]
    def dag_display_name(self) -> str:
        """
        Expression part of the ``dag_display`` name hybrid property.

        :meta private:
        """
        return case(
            (self._dag_display_property_value.is_not(None), self._dag_display_property_value),
            else_=self.dag_id,
        )

    @classmethod
    @provide_session
    def deactivate_deleted_dags(
        cls,
        bundle_name: str,
        rel_filelocs: list[str],
        session: Session = NEW_SESSION,
    ) -> bool:
        """
        Set ``is_active=False`` on the DAGs for which the DAG files have been removed.

        :param bundle_name: bundle for filelocs
        :param rel_filelocs: relative filelocs for bundle
        :param session: ORM Session
        :return: True if any DAGs were marked as stale, False otherwise
        """
        log.debug("Deactivating DAGs (for which DAG files are deleted) from %s table ", cls.__tablename__)
        dag_models = session.scalars(
            select(cls)
            .where(
                cls.bundle_name == bundle_name,
            )
            .options(
                load_only(
                    cls.relative_fileloc,
                    cls.is_stale,
                ),
            )
        )

        any_deactivated = False
        for dm in dag_models:
            if dm.relative_fileloc not in rel_filelocs:
                dm.is_stale = True
                any_deactivated = True

        return any_deactivated

    @classmethod
    def dags_needing_dagruns(cls, session: Session) -> tuple[Any, dict[str, datetime]]:
        """
        Return (and lock) a list of Dag objects that are due to create a new DagRun.

        This will return a resultset of rows that is row-level-locked with a "SELECT ... FOR UPDATE" query,
        you should ensure that any scheduling decisions are made in a single transaction -- as soon as the
        transaction is committed it will be unlocked.

        :meta private:
        """
        from airflow.models.serialized_dag import SerializedDagModel

        evaluator = AssetEvaluator(session)

        def dag_ready(dag_id: str, cond: SerializedAssetBase, statuses: dict[UKey, bool]) -> bool:
            try:
                return evaluator.run(cond, statuses)
            except AttributeError:
                # if dag was serialized before 2.9 and we *just* upgraded,
                # we may be dealing with old version.  In that case,
                # just wait for the dag to be reserialized.
                log.warning("Dag '%s' has old serialization; skipping run creation.", dag_id)
                return False
            except Exception:
                log.exception("Dag '%s' failed to be evaluated; assuming not ready", dag_id)
                return False

        # this loads all the ADRQ records.... may need to limit num dags
        adrq_by_dag: dict[str, list[AssetDagRunQueue]] = defaultdict(list)
        for adrq in session.scalars(select(AssetDagRunQueue).options(joinedload(AssetDagRunQueue.dag_model))):
            if adrq.dag_model.asset_expression is None:
                # The dag referenced does not actually depend on an asset! This
                # could happen if the dag DID depend on an asset at some point,
                # but no longer does. Delete the stale adrq.
                session.delete(adrq)
            else:
                adrq_by_dag[adrq.target_dag_id].append(adrq)

        dag_statuses: dict[str, dict[UKey, bool]] = {
            dag_id: {SerializedAssetUniqueKey.from_asset(adrq.asset): True for adrq in adrqs}
            for dag_id, adrqs in adrq_by_dag.items()
        }
        ser_dags = SerializedDagModel.get_latest_serialized_dags(dag_ids=list(dag_statuses), session=session)
        for ser_dag in ser_dags:
            dag_id = ser_dag.dag_id
            statuses = dag_statuses[dag_id]
            if not dag_ready(dag_id, cond=ser_dag.dag.timetable.asset_condition, statuses=statuses):
                del adrq_by_dag[dag_id]
                del dag_statuses[dag_id]
        del dag_statuses

        # triggered dates for asset triggered dags
        triggered_date_by_dag: dict[str, datetime] = {
            dag_id: max(adrq.created_at for adrq in adrqs) for dag_id, adrqs in adrq_by_dag.items()
        }
        del adrq_by_dag

        asset_triggered_dag_ids = set(triggered_date_by_dag.keys())
        if asset_triggered_dag_ids:
            # exclude as max active runs has been reached
            exclusion_list = set(
                session.scalars(
                    select(DagModel.dag_id)
                    .join(DagRun.dag_model)
                    .where(DagRun.state.in_((DagRunState.QUEUED, DagRunState.RUNNING)))
                    .where(DagModel.dag_id.in_(asset_triggered_dag_ids))
                    .group_by(DagModel.dag_id)
                    .having(func.count() >= func.max(DagModel.max_active_runs))
                )
            )
            if exclusion_list:
                asset_triggered_dag_ids -= exclusion_list
                triggered_date_by_dag = {
                    k: v for k, v in triggered_date_by_dag.items() if k not in exclusion_list
                }

        # We limit so that _one_ scheduler doesn't try to do all the creation of dag runs
        query = (
            select(cls)
            .where(
                cls.is_paused == expression.false(),
                cls.is_stale == expression.false(),
                cls.has_import_errors == expression.false(),
                cls.exceeds_max_non_backfill == expression.false(),
                or_(
                    cls.next_dagrun_create_after <= func.now(),
                    cls.dag_id.in_(asset_triggered_dag_ids),
                ),
            )
            .order_by(cls.next_dagrun_create_after)
            .limit(cls.NUM_DAGS_PER_DAGRUN_QUERY)
        )

        return (
            session.scalars(with_row_locks(query, of=cls, session=session, skip_locked=True)),
            triggered_date_by_dag,
        )

    def calculate_dagrun_date_fields(
        self,
        dag: SerializedDAG | LazyDeserializedDAG,
        last_automated_dag_run: None | DataInterval,
    ) -> None:
        """
        Calculate ``next_dagrun`` and `next_dagrun_create_after``.

        :param dag: The DAG object
        :param last_automated_dag_run: DataInterval of most recent run of this dag, or none
            if not yet scheduled.
        """
        if isinstance(last_automated_dag_run, datetime):
            raise ValueError(
                "Passing a datetime to `DagModel.calculate_dagrun_date_fields` is not supported. "
                "Provide a data interval instead."
            )
        next_dagrun_info = dag.next_dagrun_info(last_automated_dagrun=last_automated_dag_run)
        if next_dagrun_info is None:
            self.next_dagrun_data_interval = self.next_dagrun = self.next_dagrun_create_after = None
        else:
            self.next_dagrun_data_interval = next_dagrun_info.data_interval
            self.next_dagrun = next_dagrun_info.logical_date
            self.next_dagrun_create_after = next_dagrun_info.run_after

        log.info(
            "Setting next_dagrun for %s to %s, run_after=%s",
            dag.dag_id,
            self.next_dagrun,
            self.next_dagrun_create_after,
        )

    @provide_session
    def get_asset_triggered_next_run_info(
        self, *, session: Session = NEW_SESSION
    ) -> dict[str, int | str] | None:
        if self.asset_expression is None:
            return None

        # When an asset alias does not resolve into assets, get_asset_triggered_next_run_info returns
        # an empty dict as there's no asset info to get. This method should thus return None.
        return get_asset_triggered_next_run_info([self.dag_id], session=session).get(self.dag_id, None)

    @staticmethod
    @provide_session
    def get_team_name(dag_id: str, session: Session = NEW_SESSION) -> str | None:
        """Return the team name associated to a Dag or None if it is not owned by a specific team."""
        stmt = (
            select(Team.name)
            .join(DagBundleModel.teams)
            .join(DagModel, DagModel.bundle_name == DagBundleModel.name)
            .where(DagModel.dag_id == dag_id)
        )
        return session.scalar(stmt)

    @staticmethod
    @provide_session
    def get_dag_id_to_team_name_mapping(
        dag_ids: list[str], session: Session = NEW_SESSION
    ) -> dict[str, str | None]:
        stmt = (
            select(DagModel.dag_id, Team.name)
            .join(DagBundleModel.teams)
            .join(DagModel, DagModel.bundle_name == DagBundleModel.name)
            .where(DagModel.dag_id.in_(dag_ids))
        )
        return {dag_id: team_name for dag_id, team_name in session.execute(stmt)}


STATICA_HACK = True
globals()["kcah_acitats"[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.models.serialized_dag import SerializedDagModel

    DagModel.serialized_dag = relationship(SerializedDagModel)
    """:sphinx-autoapi-skip:"""


def __getattr__(name: str):
    # Add DAG and dag for compatibility. We can't do this in
    # airflow/models/__init__.py since this module contains other things.
    if name not in ("DAG", "dag"):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    import warnings

    from airflow.utils.deprecation_tools import DeprecatedImportWarning

    warnings.warn(
        f"Import {name!r} directly from the airflow module is deprecated and "
        f"will be removed in the future. Please import it from 'airflow.sdk'.",
        DeprecatedImportWarning,
        stacklevel=2,
    )

    import airflow.sdk

    return getattr(airflow.sdk, name)
