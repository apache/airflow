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
import enum
import functools
import logging
import socket
import string
import textwrap
from functools import wraps
from typing import TYPE_CHECKING, Callable, Dict, List, NamedTuple, Optional, TypeVar, cast

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, InvalidStatsNameException
from airflow.typing_compat import Protocol

log = logging.getLogger(__name__)


class MetricType(str, enum.Enum):
    """Metrics types."""

    COUNTER = "COUNTER"
    GAUGE = "GAUGE"
    TIMER = "TIMER"


class Metric(NamedTuple):
    """StatsD metrics definition"""

    metric_type: str
    key: str
    description: str


METRICS_LIST: List[Metric] = [
    Metric(
        metric_type=MetricType.COUNTER,
        key="{job_name}_start",
        description="Number of started ``{job_name}`` job, ex. ``SchedulerJob``, ``LocalTaskJob``",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="{job_name}_end",
        description="Number of ended ``{job_name}`` job, ex. ``SchedulerJob``, ``LocalTaskJob``",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="operator_failures_{operator_name}",
        description="Operator ``{operator_name}`` failures",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="operator_successes_{operator_name}",
        description="Operator ``{operator_name}`` successes",
    ),
    Metric(metric_type=MetricType.COUNTER, key="ti_failures", description="Overall task instances failures"),
    Metric(
        metric_type=MetricType.COUNTER, key="ti_successes", description="Overall task instances successes"
    ),
    Metric(metric_type=MetricType.COUNTER, key="zombies_killed", description="Zombie tasks killed"),
    Metric(metric_type=MetricType.COUNTER, key="scheduler_heartbeat", description="Scheduler heartbeats"),
    Metric(
        metric_type=MetricType.COUNTER,
        key="dag_processing.processes",
        description="Number of currently running DAG parsing processes",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="scheduler.tasks.killed_externally",
        description="Number of tasks killed externally",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="scheduler.tasks.running",
        description="Number of tasks running in executor",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="scheduler.tasks.starving",
        description="Number of tasks that cannot be scheduled because of no open slot in pool",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="scheduler.orphaned_tasks.cleared",
        description="Number of Orphaned tasks cleared by the Scheduler",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="scheduler.orphaned_tasks.adopted",
        description="Number of Orphaned tasks adopted by the Scheduler",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="scheduler.critical_section_busy",
        description=(
            "Count of times a scheduler process tried to get a lock on the critical section (needed to send "
            "tasks to the executor) and found it locked by another process."
        ),
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="sla_email_notification_failure",
        description="Number of failed SLA miss email notification attempts",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="ti.start.{dagid}.{taskid}",
        description="Number of started task in a given dag. Similar to <job_name>_start but for task",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="ti.finish.{dagid}.{taskid}.{state}",
        description="Number of completed task in a given dag. Similar to <job_name>_end but for task",
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="dag.callback_exceptions",
        description=(
            "Number of exceptions raised from DAG callbacks. When this happens, it means DAG callback is "
            "not working."
        ),
    ),
    Metric(
        metric_type=MetricType.COUNTER,
        key="celery.task_timeout_error",
        description=("Number of ``AirflowTaskTimeout`` errors raised when publishing Task to Celery Broker."),
    ),
    Metric(metric_type=MetricType.GAUGE, key="dagbag_size", description="DAG bag size"),
    Metric(
        metric_type=MetricType.GAUGE,
        key="dag_processing.import_errors",
        description="Number of errors from trying to parse DAG files",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="dag_processing.total_parse_time",
        description="Seconds taken to scan and import all DAG files once",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="dag_processing.last_runtime.{dag_file}",
        description="Seconds spent processing ``<dag_file>`` (in most recent iteration)",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="dag_processing.last_run.seconds_ago.{dag_file}",
        description="Seconds since ``<dag_file>`` was last processed",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="dag_processing.processor_timeouts",
        description="Number of file processors that have been killed due to taking too long",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="executor.open_slots",
        description="Number of open slots on executor",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="executor.queued_tasks",
        description="Number of queued tasks on executor",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="executor.running_tasks",
        description="Number of running tasks on executor",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="pool.open_slots.{pool_name}",
        description="Number of open slots in the pool",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="pool.queued_slots.{pool_name}",
        description="Number of queued slots in the pool",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="pool.running_slots.{pool_name}",
        description="Number of running slots in the pool",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="pool.starving_tasks.{pool_name}",
        description="Number of starving tasks in the pool",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="smart_sensor_operator.poked_tasks",
        description="Number of tasks poked by the smart sensor in the previous poking loop",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="smart_sensor_operator.poked_success",
        description="Number of newly succeeded tasks poked by the smart sensor in the previous poking loop",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="smart_sensor_operator.poked_exception",
        description="Number of exceptions in the previous smart sensor poking loop",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="smart_sensor_operator.exception_failures",
        description="Number of failures caused by exception in the previous smart sensor poking loop",
    ),
    Metric(
        metric_type=MetricType.GAUGE,
        key="smart_sensor_operator.infra_failures",
        description="Number of infrastructure failures in the previous smart sensor poking loop",
    ),
    Metric(
        metric_type=MetricType.TIMER,
        key="dagrun.dependency-check.{dag_id}",
        description="Milliseconds taken to check DAG dependencies",
    ),
    Metric(
        metric_type=MetricType.TIMER,
        key="dag.{dag_id}.{task_id}.duration",
        description="Milliseconds taken to finish a task",
    ),
    Metric(
        metric_type=MetricType.TIMER,
        key="dag_processing.last_duration.{dag_file}",
        description="Milliseconds taken to load the given DAG file",
    ),
    Metric(
        metric_type=MetricType.TIMER,
        key="dagrun.duration.success.{dag_id}",
        description="Milliseconds taken for a DagRun to reach success state",
    ),
    Metric(
        metric_type=MetricType.TIMER,
        key="dagrun.duration.failed.{dag_id}",
        description="Milliseconds taken for a DagRun to reach failed state",
    ),
    Metric(
        metric_type=MetricType.TIMER,
        key="dagrun.schedule_delay.{dag_id}",
        description=(
            "Milliseconds of delay between the scheduled DagRun start date and the actual "
            "DagRun start date"
        ),
    ),
    Metric(
        metric_type=MetricType.TIMER,
        key="scheduler.critical_section_duration",
        description=(
            "Milliseconds spent in the critical section of scheduler loop -- only a single scheduler "
            "can enter this loop at a time"
        ),
    ),
]


class TimerProtocol(Protocol):
    """Type protocol for StatsLogger.timer"""

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_value, traceback):
        ...

    def start(self):
        """Start the timer"""
        ...

    def stop(self, send=True):
        """Stop, and (by default) submit the timer to statsd"""
        ...


class StatsLogger(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, pylint, etc)"""

    @classmethod
    def incr(
        cls, stat: str, count: int = 1, rate: int = 1, *, labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Increment stat"""

    @classmethod
    def decr(
        cls, stat: str, count: int = 1, rate: int = 1, *, labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Decrement stat"""

    @classmethod
    def gauge(
        cls,
        stat: str,
        value: float,
        rate: int = 1,
        delta: bool = False,
        *,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Gauge stat"""

    @classmethod
    def timing(cls, stat: str, dt, *, labels: Optional[Dict[str, str]] = None) -> None:
        """Stats timing"""

    @classmethod
    def timer(cls, stat, *, labels: Optional[Dict[str, str]] = None) -> TimerProtocol:
        """Timer metric that can be cancelled"""


class DummyTimer:
    """No-op timer"""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self

    def start(self):
        """Start the timer"""

    def stop(self, send=True):  # pylint: disable=unused-argument
        """Stop, and (by default) submit the timer to statsd"""


class DummyStatsLogger:
    """If no StatsLogger is configured, DummyStatsLogger is used as a fallback"""

    @classmethod
    def incr(cls, stat, count=1, rate=1, *, labels=None):
        """Increment stat"""

    @classmethod
    def decr(cls, stat, count=1, rate=1, *, labels=None):
        """Decrement stat"""

    @classmethod
    def gauge(cls, stat, value, rate=1, delta=False, *, labels=None):
        """Gauge stat"""

    @classmethod
    def timing(cls, stat, dt, *, labels=None):
        """Stats timing"""

    @classmethod
    def timer(cls, *args, **kwargs):
        """Timer metric that can be cancelled"""
        return DummyTimer()


# Only characters in the character set are considered valid
# for the stat_name if stat_name_default_handler is used.
ALLOWED_CHARACTERS = set(string.ascii_letters + string.digits + '_.-')


def stat_name_default_handler(stat_name, max_length=250) -> str:
    """A function that validate the statsd stat name, apply changes to the stat name
    if necessary and return the transformed stat name.
    """
    if not isinstance(stat_name, str):
        raise InvalidStatsNameException('The stat_name has to be a string')
    if len(stat_name) > max_length:
        raise InvalidStatsNameException(
            textwrap.dedent(
                """\
            The stat_name ({stat_name}) has to be less than {max_length} characters.
        """.format(
                    stat_name=stat_name, max_length=max_length
                )
            )
        )
    if not all((c in ALLOWED_CHARACTERS) for c in stat_name):
        raise InvalidStatsNameException(
            textwrap.dedent(
                """\
            The stat name ({stat_name}) has to be composed with characters in
            {allowed_characters}.
            """.format(
                    stat_name=stat_name, allowed_characters=ALLOWED_CHARACTERS
                )
            )
        )
    return stat_name


def get_current_handler_stat_name_func() -> Callable[[str], str]:
    """Get Stat Name Handler from airflow.cfg"""
    return conf.getimport('scheduler', 'stat_name_handler') or stat_name_default_handler


T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def validate_stat(fn: T) -> T:
    """Check if stat name contains invalid characters.
    Log and not emit stats if name is invalid
    """

    @wraps(fn)
    def wrapper(_self, stat, *args, **kwargs):
        try:
            handler_stat_name_func = get_current_handler_stat_name_func()
            stat_name = handler_stat_name_func(stat)
            return fn(_self, stat_name, *args, **kwargs)
        except InvalidStatsNameException:
            log.error('Invalid stat name: %s.', stat, exc_info=True)
            return None

    return cast(T, wrapper)


class AllowListValidator:
    """Class to filter unwanted stats"""

    def __init__(self, allow_list=None):
        if allow_list:
            self.allow_list = tuple([item.strip().lower() for item in allow_list.split(',')])
        else:
            self.allow_list = None

    def test(self, stat):
        """Test if stat is in the Allow List"""
        if self.allow_list is not None:
            return stat.strip().lower().startswith(self.allow_list)
        else:
            return True  # default is all metrics allowed


def _format_safe_stats_logger_args(func):
    @functools.wraps(func)
    def func_wrap(_self, stat, *args, labels=None, **kwargs):
        if labels:
            # Remove {} from stat key e.g. {class_name} => class_name
            stat = stat.format(**labels)
        return validate_stat(func)(_self, stat, *args, **kwargs)

    return func_wrap


class SafeStatsdLogger:
    """Statsd Logger"""

    def __init__(self, statsd_client, allow_list_validator=AllowListValidator()):
        self.statsd = statsd_client
        self.allow_list_validator = allow_list_validator

    @_format_safe_stats_logger_args
    def incr(self, stat, count=1, rate=1, labels=None):
        """Increment stat"""
        del labels
        if self.allow_list_validator.test(stat):
            return self.statsd.incr(stat, count, rate)
        return None

    @_format_safe_stats_logger_args
    def decr(self, stat, count=1, rate=1, labels=None):
        """Decrement stat"""
        del labels

        if self.allow_list_validator.test(stat):
            return self.statsd.decr(stat, count, rate)
        return None

    @_format_safe_stats_logger_args
    def gauge(self, stat, value, rate=1, delta=False, labels=None):
        """Gauge stat"""
        del labels

        if self.allow_list_validator.test(stat):
            return self.statsd.gauge(stat, value, rate, delta)
        return None

    @_format_safe_stats_logger_args
    def timing(self, stat, dt, labels=None):
        """Stats timing"""
        del labels

        if self.allow_list_validator.test(stat):
            return self.statsd.timing(stat, dt)
        return None

    @_format_safe_stats_logger_args
    def timer(self, stat, *args, labels=None, **kwargs):
        """Timer metric that can be cancelled"""
        del labels

        if self.allow_list_validator.test(stat):
            return self.statsd.timer(stat, *args, **kwargs)
        return DummyTimer()


def _format_safe_datadog_stats_logger_args(func):
    @functools.wraps(func)
    def func_wrap(_self, stat, *args, labels=None, tags=None, **kwargs):
        tags = tags or []
        if labels:
            # Remove {} from stat key e.g. {class_name} => class_name
            stat = stat.format(**{k: k for k in labels})
            tags += [f"{k}:{v}" for k, v in labels.items()]
        return validate_stat(func)(_self, stat, *args, tags=tags, **kwargs)

    return func_wrap


class SafeDogStatsdLogger:
    """DogStatsd Logger"""

    def __init__(self, dogstatsd_client, allow_list_validator=AllowListValidator()):
        self.dogstatsd = dogstatsd_client
        self.allow_list_validator = allow_list_validator

    @_format_safe_datadog_stats_logger_args
    def incr(self, stat, count=1, rate=1, tags=None):
        """Increment stat"""
        if self.allow_list_validator.test(stat):
            return self.dogstatsd.increment(metric=stat, value=count, tags=tags, sample_rate=rate)
        return None

    @_format_safe_datadog_stats_logger_args
    def decr(self, stat, count=1, rate=1, tags=None):
        """Decrement stat"""
        if self.allow_list_validator.test(stat):
            return self.dogstatsd.decrement(metric=stat, value=count, tags=tags, sample_rate=rate)
        return None

    @_format_safe_datadog_stats_logger_args
    def gauge(self, stat, value, rate=1, delta=False, tags=None):  # pylint: disable=unused-argument
        """Gauge stat"""
        if self.allow_list_validator.test(stat):
            return self.dogstatsd.gauge(metric=stat, value=value, tags=tags, sample_rate=rate)
        return None

    @_format_safe_datadog_stats_logger_args
    def timing(self, stat, dt, tags=None):
        """Stats timing"""
        if self.allow_list_validator.test(stat):
            return self.dogstatsd.timing(metric=stat, value=dt, tags=tags)
        return None

    @_format_safe_datadog_stats_logger_args
    def timer(self, stat, *args, tags=None, **kwargs):
        """Timer metric that can be cancelled"""
        if self.allow_list_validator.test(stat):
            return self.dogstatsd.timer(stat, *args, tags=tags, **kwargs)
        return DummyTimer()


class _Stats(type):
    instance: Optional[StatsLogger] = None

    def __getattr__(cls, name):
        return getattr(cls.instance, name)

    def __init__(cls, *args, **kwargs):
        super().__init__(cls)
        if cls.__class__.instance is None:
            try:
                is_datadog_enabled_defined = conf.has_option('scheduler', 'statsd_datadog_enabled')
                if is_datadog_enabled_defined and conf.getboolean('scheduler', 'statsd_datadog_enabled'):
                    cls.__class__.instance = cls.get_dogstatsd_logger()
                elif conf.getboolean('scheduler', 'statsd_on'):
                    cls.__class__.instance = cls.get_statsd_logger()
                else:
                    cls.__class__.instance = DummyStatsLogger()
            except (socket.gaierror, ImportError) as e:
                log.error("Could not configure StatsClient: %s, using DummyStatsLogger instead.", e)
                cls.__class__.instance = DummyStatsLogger()

    @classmethod
    def get_statsd_logger(cls) -> StatsLogger:
        """Returns logger for statsd"""
        # no need to check for the scheduler/statsd_on -> this method is only called when it is set
        # and previously it would crash with None is callable if it was called without it.
        from statsd import StatsClient

        stats_class = conf.getimport('scheduler', 'statsd_custom_client_path')
        if stats_class:
            if not issubclass(stats_class, StatsClient):
                raise AirflowConfigException(
                    "Your custom Statsd client must extend the statsd.StatsClient in order to ensure "
                    "backwards compatibility."
                )
            else:
                log.info("Successfully loaded custom Statsd client")

        else:
            stats_class = StatsClient

        statsd = stats_class(
            host=conf.get('scheduler', 'statsd_host'),
            port=conf.getint('scheduler', 'statsd_port'),
            prefix=conf.get('scheduler', 'statsd_prefix'),
        )
        allow_list_validator = AllowListValidator(conf.get('scheduler', 'statsd_allow_list', fallback=None))
        return SafeStatsdLogger(statsd, allow_list_validator)

    @classmethod
    def get_dogstatsd_logger(cls) -> StatsLogger:
        """Get DataDog statsd logger"""
        from datadog import DogStatsd

        dogstatsd = DogStatsd(
            host=conf.get('scheduler', 'statsd_host'),
            port=conf.getint('scheduler', 'statsd_port'),
            namespace=conf.get('scheduler', 'statsd_prefix'),
            constant_tags=cls.get_constant_tags(),
        )
        dogstatsd_allow_list = conf.get('scheduler', 'statsd_allow_list', fallback=None)
        allow_list_validator = AllowListValidator(dogstatsd_allow_list)
        return SafeDogStatsdLogger(dogstatsd, allow_list_validator)

    @classmethod
    def get_constant_tags(cls):
        """Get constant DataDog tags to add to all stats"""
        tags = []
        tags_in_string = conf.get('scheduler', 'statsd_datadog_tags', fallback=None)
        if tags_in_string is None or tags_in_string == '':
            return tags
        else:
            for key_value in tags_in_string.split(','):
                tags.append(key_value)
            return tags


if TYPE_CHECKING:
    Stats: StatsLogger
else:

    class Stats(metaclass=_Stats):  # noqa: D101
        """Empty class for Stats - we use metaclass to inject the right one"""
