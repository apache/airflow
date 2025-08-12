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

import functools
import logging
from collections import defaultdict
from collections.abc import Callable, Collection
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, TypeVar, Union

import attrs
import methodtools
import sqlalchemy_jsonfield
from dateutil.relativedelta import relativedelta
from sqlalchemy import (
    Boolean,
    Column,
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
from sqlalchemy.orm import backref, load_only, relationship
from sqlalchemy.sql import expression

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.assets.evaluation import AssetEvaluator
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException
from airflow.models.asset import AssetDagRunQueue, AssetModel
from airflow.models.base import Base, StringID
from airflow.models.dagrun import DagRun
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetUniqueKey, BaseAsset
from airflow.sdk.definitions.dag import DAG as TaskSDKDag, dag as task_sdk_dag_decorator
from airflow.sdk.definitions.deadline import DeadlineAlert
from airflow.settings import json
from airflow.timetables.base import DataInterval, Timetable
from airflow.utils.context import Context
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, with_row_locks
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from typing import TypeAlias

    import pendulum
    from sqlalchemy.orm.query import Query
    from sqlalchemy.orm.session import Session

    from airflow.models.mappedoperator import MappedOperator
    from airflow.serialization.serialized_objects import MaybeSerializedDAG, SerializedBaseOperator

    Operator: TypeAlias = MappedOperator | SerializedBaseOperator

log = logging.getLogger(__name__)

AssetT = TypeVar("AssetT", bound=BaseAsset)

TAG_MAX_LEN = 100

DagStateChangeCallback = Callable[[Context], None]
ScheduleInterval = None | str | timedelta | relativedelta

ScheduleArg = ScheduleInterval | Timetable | BaseAsset | Collection[Union["Asset", "AssetAlias"]]


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


def get_last_dagrun(dag_id, session, include_manually_triggered=False):
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


if TYPE_CHECKING:
    dag = task_sdk_dag_decorator
else:

    def dag(dag_id: str = "", **kwargs):
        return task_sdk_dag_decorator(dag_id, __DAG_class=DAG, __warnings_stacklevel_delta=3, **kwargs)


@functools.total_ordering
@attrs.define(hash=False, repr=False, eq=False, slots=False)
class DAG(TaskSDKDag, LoggingMixin):
    """
    A dag (directed acyclic graph) is a collection of tasks with directional dependencies.

    A dag also has a schedule, a start date and an end date (optional).  For each schedule,
    (say daily or hourly), the DAG needs to run each individual tasks as their dependencies
    are met. Certain tasks have the property of depending on their own past, meaning that
    they can't run until their previous schedule (and upstream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    Note that if you plan to use time zones all the dates provided should be pendulum
    dates. See :ref:`timezone_aware_dags`.

    .. versionadded:: 2.4
        The *schedule* argument to specify either time-based scheduling logic
        (timetable), or asset-driven triggers.

    .. versionchanged:: 3.0
        The default value of *schedule* has been changed to *None* (no schedule).
        The previous default was ``timedelta(days=1)``.

    :param dag_id: The id of the DAG; must consist exclusively of alphanumeric
        characters, dashes, dots and underscores (all ASCII)
    :param description: The description for the DAG to e.g. be shown on the webserver
    :param schedule: If provided, this defines the rules according to which DAG
        runs are scheduled. Possible values include a cron expression string,
        timedelta object, Timetable, or list of Asset objects.
        See also :doc:`/howto/timetable`.
    :param start_date: The timestamp from which the scheduler will
        attempt to backfill. If this is not provided, backfilling must be done
        manually with an explicit time range.
    :param end_date: A date beyond which your DAG won't run, leave to None
        for open-ended scheduling.
    :param template_searchpath: This list of folders (non-relative)
        defines where jinja will look for your templates. Order matters.
        Note that jinja/airflow includes the path of your DAG file by
        default
    :param template_undefined: Template undefined type.
    :param user_defined_macros: a dictionary of macros that will be exposed
        in your jinja templates. For example, passing ``dict(foo='bar')``
        to this argument allows you to ``{{ foo }}`` in all jinja
        templates related to this DAG. Note that you can pass any
        type of object here.
    :param user_defined_filters: a dictionary of filters that will be exposed
        in your jinja templates. For example, passing
        ``dict(hello=lambda name: 'Hello %s' % name)`` to this argument allows
        you to ``{{ 'world' | hello }}`` in all jinja templates related to
        this DAG.
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :param params: a dictionary of DAG level parameters that are made
        accessible in templates, namespaced under `params`. These
        params can be overridden at the task level.
    :param max_active_tasks: the number of task instances allowed to run
        concurrently
    :param max_active_runs: maximum number of active DAG runs, beyond this
        number of DAG runs in a running state, the scheduler won't create
        new active DAG runs
    :param max_consecutive_failed_dag_runs: (experimental) maximum number of consecutive failed DAG runs,
        beyond this the scheduler will disable the DAG
    :param dagrun_timeout: Specify the duration a DagRun should be allowed to run before it times out or
        fails. Task instances that are running when a DagRun is timed out will be marked as skipped.
    :param sla_miss_callback: DEPRECATED - The SLA feature is removed in Airflow 3.0, to be replaced with a new implementation in 3.1
    :param deadline: Optional Deadline Alert for the DAG.
        Specifies a time by which the DAG run should be complete, either in the form of a static datetime
        or calculated relative to a reference timestamp.  If the deadline passes before completion, the
        provided callback is triggered.

        **Example**: To set the deadline for one hour after the DAG run starts you could use ::

            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
                interval=timedelta(hours=1),
                callback=my_callback,
            )

    :param catchup: Perform scheduler catchup (or only run latest)? Defaults to False
    :param on_failure_callback: A function or list of functions to be called when a DagRun of this dag fails.
        A context dictionary is passed as a single parameter to this function.
    :param on_success_callback: Much like the ``on_failure_callback`` except
        that it is executed when the dag succeeds.
    :param access_control: Specify optional DAG-level actions, e.g.,
        "{'role1': {'can_read'}, 'role2': {'can_read', 'can_edit', 'can_delete'}}"
        or it can specify the resource name if there is a DAGs Run resource, e.g.,
        "{'role1': {'DAG Runs': {'can_create'}}, 'role2': {'DAGs': {'can_read', 'can_edit', 'can_delete'}}"
    :param is_paused_upon_creation: Specifies if the dag is paused when created for the first time.
        If the dag exists already, this flag will be ignored. If this optional parameter
        is not specified, the global config setting will be used.
    :param jinja_environment_kwargs: additional configuration options to be passed to Jinja
        ``Environment`` for template rendering

        **Example**: to avoid Jinja from removing a trailing newline from template strings ::

            DAG(
                dag_id="my-dag",
                jinja_environment_kwargs={
                    "keep_trailing_newline": True,
                    # some other jinja2 Environment options here
                },
            )

        **See**: `Jinja Environment documentation
        <https://jinja.palletsprojects.com/en/2.11.x/api/#jinja2.Environment>`_

    :param render_template_as_native_obj: If True, uses a Jinja ``NativeEnvironment``
        to render templates as native Python types. If False, a Jinja
        ``Environment`` is used to render templates as string values.
    :param tags: List of tags to help filtering DAGs in the UI.
    :param owner_links: Dict of owners and their links, that will be clickable on the DAGs view UI.
        Can be used as an HTTP link (for example the link to your Slack channel), or a mailto link.
        e.g: {"dag_owner": "https://airflow.apache.org/"}
    :param auto_register: Automatically register this DAG when it is used in a ``with`` block
    :param fail_fast: Fails currently running tasks when task in DAG fails.
        **Warning**: A fail fast dag can only have tasks with the default trigger rule ("all_success").
        An exception will be thrown if any task in a fail fast dag has a non default trigger rule.
    :param dag_display_name: The display name of the DAG which appears on the UI.
    """

    partial: bool = False

    @property
    def safe_dag_id(self):
        return self.dag_id.replace(".", "__dot__")

    @property
    def dag_id(self) -> str:
        return self._dag_id

    @dag_id.setter
    def dag_id(self, value: str) -> None:
        self._dag_id = value

    @provide_session
    def get_is_active(self, session=NEW_SESSION) -> None:
        """Return a boolean indicating whether this DAG is active."""
        return session.scalar(select(~DagModel.is_stale).where(DagModel.dag_id == self.dag_id))

    @provide_session
    def get_is_stale(self, session=NEW_SESSION) -> None:
        """Return a boolean indicating whether this DAG is stale."""
        return session.scalar(select(DagModel.is_stale).where(DagModel.dag_id == self.dag_id))

    @methodtools.lru_cache(maxsize=None)
    @classmethod
    def get_serialized_fields(cls):
        """Stringified DAGs and operators contain exactly these fields."""
        return TaskSDKDag.get_serialized_fields() | {"_processor_dags_folder"}

    @staticmethod
    @provide_session
    def fetch_dagrun(dag_id: str, run_id: str, session: Session = NEW_SESSION) -> DagRun:
        """
        Return the dag run for a given run_id if it exists, otherwise none.

        :param dag_id: The dag_id of the DAG to find.
        :param run_id: The run_id of the DagRun to find.
        :param session:
        :return: The DagRun if found, otherwise None.
        """
        return session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id))

    @provide_session
    def get_dagrun(self, run_id: str, session: Session = NEW_SESSION) -> DagRun:
        return DAG.fetch_dagrun(dag_id=self.dag_id, run_id=run_id, session=session)

    @provide_session
    def get_latest_logical_date(self, session: Session = NEW_SESSION) -> pendulum.DateTime | None:
        """Return the latest date for which at least one dag run exists."""
        return session.scalar(select(func.max(DagRun.logical_date)).where(DagRun.dag_id == self.dag_id))

    @staticmethod
    @provide_session
    def deactivate_stale_dags(expiration_date, session=NEW_SESSION):
        """
        Deactivate any DAGs that were last touched by the scheduler before the expiration date.

        These DAGs were likely deleted.

        :param expiration_date: set inactive DAGs that were touched before this time
        :return: None
        """
        for dag in session.scalars(
            select(DagModel).where(DagModel.last_parsed_time < expiration_date, ~DagModel.is_stale)
        ):
            log.info(
                "Deactivating DAG ID %s since it was last touched by the scheduler at %s",
                dag.dag_id,
                dag.last_parsed_time.isoformat(),
            )
            dag.is_stale = True
            session.merge(dag)
            session.commit()


class DagTag(Base):
    """A tag name per dag, to allow quick filtering in the DAG view."""

    __tablename__ = "dag_tag"
    name = Column(String(TAG_MAX_LEN), primary_key=True)
    dag_id = Column(
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
    dag_id = Column(
        StringID(),
        ForeignKey("dag.dag_id", name="dag.dag_id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    owner = Column(String(500), primary_key=True, nullable=False)
    link = Column(String(500), nullable=False)

    def __repr__(self):
        return f"<DagOwnerAttributes: dag_id={self.dag_id}, owner={self.owner}, link={self.link}>"

    @classmethod
    def get_all(cls, session) -> dict[str, dict[str, str]]:
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
    dag_id = Column(StringID(), primary_key=True)
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = airflow_conf.getboolean("core", "dags_are_paused_at_creation")
    is_paused = Column(Boolean, default=is_paused_at_creation)
    # Whether that DAG was seen on the last DagBag load
    is_stale = Column(Boolean, default=True)
    # Last time the scheduler started
    last_parsed_time = Column(UtcDateTime)
    # Time when the DAG last received a refresh signal
    # (e.g. the DAG's "refresh" button was clicked in the web UI)
    last_expired = Column(UtcDateTime)
    # The location of the file containing the DAG object
    # Note: Do not depend on fileloc pointing to a file; in the case of a
    # packaged DAG, it will point to the subpath of the DAG within the
    # associated zip.
    fileloc = Column(String(2000))
    relative_fileloc = Column(String(2000))
    bundle_name = Column(StringID(), ForeignKey("dag_bundle.name"), nullable=False)
    # The version of the bundle the last time the DAG was processed
    bundle_version = Column(String(200), nullable=True)
    # String representing the owners
    owners = Column(String(2000))
    # Display name of the dag
    _dag_display_property_value = Column("dag_display_name", String(2000), nullable=True)
    # Description of the dag
    description = Column(Text)
    # Timetable summary
    timetable_summary = Column(Text, nullable=True)
    # Timetable description
    timetable_description = Column(String(1000), nullable=True)
    # Asset expression based on asset triggers
    asset_expression = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    # DAG deadline information
    _deadline = Column("deadline", sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    # Tags for view filter
    tags = relationship("DagTag", cascade="all, delete, delete-orphan", backref=backref("dag"))
    # Dag owner links for DAGs view
    dag_owner_links = relationship(
        "DagOwnerAttributes", cascade="all, delete, delete-orphan", backref=backref("dag")
    )

    max_active_tasks = Column(Integer, nullable=False)
    max_active_runs = Column(Integer, nullable=True)  # todo: should not be nullable if we have a default
    max_consecutive_failed_dag_runs = Column(Integer, nullable=False)

    has_task_concurrency_limits = Column(Boolean, nullable=False)
    has_import_errors = Column(Boolean(), default=False, server_default="0")

    # The logical date of the next dag run.
    next_dagrun = Column(UtcDateTime)

    # Must be either both NULL or both datetime.
    next_dagrun_data_interval_start = Column(UtcDateTime)
    next_dagrun_data_interval_end = Column(UtcDateTime)

    # Earliest time at which this ``next_dagrun`` can be created.
    next_dagrun_create_after = Column(UtcDateTime)

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
        return DeadlineAlert.deserialize_deadline_alert(self._deadline) if self._deadline else None

    @deadline.setter
    def deadline(self, value):
        """Set and serialize the deadline alert."""
        self._deadline = value if isinstance(value, dict) else value.serialize_deadline_alert()

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
    def get_current(cls, dag_id: str, session=NEW_SESSION) -> DagModel:
        return session.scalar(select(cls).where(cls.dag_id == dag_id))

    @provide_session
    def get_last_dagrun(self, session=NEW_SESSION, include_manually_triggered=False):
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
        paused_dag_ids = session.execute(
            select(DagModel.dag_id)
            .where(DagModel.is_paused == expression.true())
            .where(DagModel.dag_id.in_(dag_ids))
        )

        paused_dag_ids = {paused_dag_id for (paused_dag_id,) in paused_dag_ids}
        return paused_dag_ids

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
    ) -> None:
        """
        Set ``is_active=False`` on the DAGs for which the DAG files have been removed.

        :param bundle_name: bundle for filelocs
        :param rel_filelocs: relative filelocs for bundle
        :param session: ORM Session
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

        for dm in dag_models:
            if dm.relative_fileloc not in rel_filelocs:
                dm.is_stale = True

    @classmethod
    def dags_needing_dagruns(cls, session: Session) -> tuple[Query, dict[str, datetime]]:
        """
        Return (and lock) a list of Dag objects that are due to create a new DagRun.

        This will return a resultset of rows that is row-level-locked with a "SELECT ... FOR UPDATE" query,
        you should ensure that any scheduling decisions are made in a single transaction -- as soon as the
        transaction is committed it will be unlocked.
        """
        from airflow.models.serialized_dag import SerializedDagModel

        evaluator = AssetEvaluator(session)

        def dag_ready(dag_id: str, cond: BaseAsset, statuses: dict[AssetUniqueKey, bool]) -> bool | None:
            # if dag was serialized before 2.9 and we *just* upgraded,
            # we may be dealing with old version.  In that case,
            # just wait for the dag to be reserialized.
            try:
                return evaluator.run(cond, statuses)
            except AttributeError:
                log.warning("dag '%s' has old serialization; skipping DAG run creation.", dag_id)
                return None

        # this loads all the ADRQ records.... may need to limit num dags
        adrq_by_dag: dict[str, list[AssetDagRunQueue]] = defaultdict(list)
        for r in session.scalars(select(AssetDagRunQueue)):
            adrq_by_dag[r.target_dag_id].append(r)

        dag_statuses: dict[str, dict[AssetUniqueKey, bool]] = {
            dag_id: {AssetUniqueKey.from_asset(adrq.asset): True for adrq in adrqs}
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
        dag: MaybeSerializedDAG,
        last_automated_dag_run: None | DataInterval,
    ) -> None:
        """
        Calculate ``next_dagrun`` and `next_dagrun_create_after``.

        :param dag: The DAG object
        :param last_automated_dag_run: DataInterval (or datetime) of most recent run of this dag, or none
            if not yet scheduled.
        """
        last_automated_data_interval: DataInterval | None
        if isinstance(last_automated_dag_run, datetime):
            raise ValueError(
                "Passing a datetime to `DagModel.calculate_dagrun_date_fields` is not supported. "
                "Provide a data interval instead."
            )
        last_automated_data_interval = last_automated_dag_run
        next_dagrun_info = dag.next_dagrun_info(last_automated_data_interval)
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
    def get_asset_triggered_next_run_info(self, *, session=NEW_SESSION) -> dict[str, int | str] | None:
        if self.asset_expression is None:
            return None

        # When an asset alias does not resolve into assets, get_asset_triggered_next_run_info returns
        # an empty dict as there's no asset info to get. This method should thus return None.
        return get_asset_triggered_next_run_info([self.dag_id], session=session).get(self.dag_id, None)


STATICA_HACK = True
globals()["kcah_acitats"[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.models.serialized_dag import SerializedDagModel

    DagModel.serialized_dag = relationship(SerializedDagModel)
    """:sphinx-autoapi-skip:"""
