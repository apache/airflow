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

import json
import logging
import textwrap
import time
from typing import TYPE_CHECKING, Any, Callable, Sequence
from urllib.parse import urlencode

from flask import request, url_for
from flask.helpers import flash
from flask_appbuilder.forms import FieldConverter
from flask_appbuilder.models.filters import BaseFilter
from flask_appbuilder.models.sqla import filters as fab_sqlafilters
from flask_appbuilder.models.sqla.filters import get_field_setup_query, set_value_to_type
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_babel import lazy_gettext
from markdown_it import MarkdownIt
from markupsafe import Markup
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter
from sqlalchemy import delete, func, select, types
from sqlalchemy.ext.associationproxy import AssociationProxy

from airflow.models.dagrun import DagRun
from airflow.models.dagwarning import DagWarning
from airflow.models.errors import ParseImportError
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.code_utils import get_python_source
from airflow.utils.helpers import alchemy_to_dict
from airflow.utils.json import WebEncoder
from airflow.utils.sqlalchemy import tuple_in_condition
from airflow.utils.state import State, TaskInstanceState
from airflow.www.extensions.init_auth_manager import get_auth_manager
from airflow.www.forms import DateTimeWithTimezoneField
from airflow.www.widgets import AirflowDateTimePickerWidget

if TYPE_CHECKING:
    from flask_appbuilder.models.sqla import Model
    from pendulum.datetime import DateTime
    from pygments.lexer import Lexer
    from sqlalchemy.orm.session import Session
    from sqlalchemy.sql import Select
    from sqlalchemy.sql.operators import ColumnOperators


TI = TaskInstance

logger = logging.getLogger(__name__)


def datetime_to_string(value: DateTime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def get_mapped_instances(task_instance, session):
    return session.scalars(
        select(TaskInstance)
        .where(
            TaskInstance.dag_id == task_instance.dag_id,
            TaskInstance.run_id == task_instance.run_id,
            TaskInstance.task_id == task_instance.task_id,
        )
        .order_by(TaskInstance.map_index)
    ).all()


def get_instance_with_map(task_instance, session):
    if task_instance.map_index == -1:
        data = alchemy_to_dict(task_instance)
        # Fetch execution_date explicitly since it's not a column and a proxy
        data["execution_date"] = task_instance.execution_date
        return data
    mapped_instances = get_mapped_instances(task_instance, session)
    return get_mapped_summary(task_instance, mapped_instances)


priority: list[None | TaskInstanceState] = [
    TaskInstanceState.FAILED,
    TaskInstanceState.UPSTREAM_FAILED,
    TaskInstanceState.UP_FOR_RETRY,
    TaskInstanceState.UP_FOR_RESCHEDULE,
    TaskInstanceState.QUEUED,
    TaskInstanceState.SCHEDULED,
    TaskInstanceState.DEFERRED,
    TaskInstanceState.RUNNING,
    TaskInstanceState.RESTARTING,
    None,
    TaskInstanceState.SUCCESS,
    TaskInstanceState.SKIPPED,
    TaskInstanceState.REMOVED,
]


def get_mapped_summary(parent_instance, task_instances):
    mapped_states = [ti.state for ti in task_instances]

    group_state = None
    for state in priority:
        if state in mapped_states:
            group_state = state
            break

    group_queued_dttm = datetime_to_string(
        min((ti.queued_dttm for ti in task_instances if ti.queued_dttm), default=None)
    )

    group_start_date = datetime_to_string(
        min((ti.start_date for ti in task_instances if ti.start_date), default=None)
    )
    group_end_date = datetime_to_string(
        max((ti.end_date for ti in task_instances if ti.end_date), default=None)
    )

    return {
        "task_id": parent_instance.task_id,
        "run_id": parent_instance.run_id,
        "state": group_state,
        "queued_dttm": group_queued_dttm,
        "start_date": group_start_date,
        "end_date": group_end_date,
        "mapped_states": mapped_states,
        "try_number": parent_instance.try_number,
        "execution_date": parent_instance.execution_date,
    }


def get_dag_run_conf(
    dag_run_conf: Any, *, json_encoder: type[json.JSONEncoder] = json.JSONEncoder
) -> tuple[str | None, bool]:
    result: str | None = None

    conf_is_json: bool = False
    if isinstance(dag_run_conf, str):
        result = dag_run_conf
    elif isinstance(dag_run_conf, (dict, list)) and any(dag_run_conf):
        result = json.dumps(dag_run_conf, sort_keys=True, cls=json_encoder, ensure_ascii=False)
        conf_is_json = True

    return result, conf_is_json


def encode_dag_run(
    dag_run: DagRun | None, *, json_encoder: type[json.JSONEncoder] = json.JSONEncoder
) -> tuple[dict[str, Any] | None, None | str]:
    if not dag_run:
        return None, None

    try:
        dag_run_conf, conf_is_json = get_dag_run_conf(dag_run.conf, json_encoder=json_encoder)
        encoded_dag_run = {
            "run_id": dag_run.run_id,
            "queued_at": datetime_to_string(dag_run.queued_at),
            "start_date": datetime_to_string(dag_run.start_date),
            "end_date": datetime_to_string(dag_run.end_date),
            "state": dag_run.state,
            "execution_date": datetime_to_string(dag_run.execution_date),
            "data_interval_start": datetime_to_string(dag_run.data_interval_start),
            "data_interval_end": datetime_to_string(dag_run.data_interval_end),
            "run_type": dag_run.run_type,
            "last_scheduling_decision": datetime_to_string(dag_run.last_scheduling_decision),
            "external_trigger": dag_run.external_trigger,
            "conf": dag_run_conf,
            "conf_is_json": conf_is_json,
            "note": dag_run.note,
            "triggered_by": dag_run.triggered_by.value,
        }
    except ValueError as e:
        logger.error("Error while encoding the DAG Run!", exc_info=e)
        if str(e) == "Circular reference detected":
            return None, (
                f"Circular reference detected in the DAG Run config (#{dag_run.run_id}). "
                f"You should check your webserver logs for more details."
            )
        else:
            raise e

    return encoded_dag_run, None


def check_import_errors(fileloc, session):
    # Check dag import errors
    import_errors = session.scalars(
        select(ParseImportError).where(ParseImportError.filename == fileloc)
    ).all()
    if import_errors:
        for import_error in import_errors:
            flash(f"Broken DAG: [{import_error.filename}] {import_error.stacktrace}", "dag_import_error")


def check_dag_warnings(dag_id, session):
    dag_warnings = session.scalars(select(DagWarning).where(DagWarning.dag_id == dag_id)).all()
    if dag_warnings:
        for dag_warning in dag_warnings:
            flash(dag_warning.message, "warning")


def get_params(**kwargs):
    """Return URL-encoded params."""
    return urlencode({d: v for d, v in kwargs.items() if v is not None}, True)


def generate_pages(
    current_page,
    num_of_pages,
    search=None,
    status=None,
    tags=None,
    window=7,
    sorting_key=None,
    sorting_direction=None,
):
    """
    Generate the HTML for a paging component.

    Uses a similar logic to the paging auto-generated by Flask managed views. The paging
    component defines a number of pages visible in the pager (window) and once the user
    goes to a page beyond the largest visible, it would scroll to the right the page numbers
    and keeps the current one in the middle of the pager component. When in the last pages,
    the pages won't scroll and just keep moving until the last page. Pager also contains
    <first, previous, ..., next, last> pages.
    This component takes into account custom parameters such as search, status, and tags
    which could be added to the pages link in order to maintain the state between
    client and server. It also allows to make a bookmark on a specific paging state.

    :param current_page: the current page number, 0-indexed
    :param num_of_pages: the total number of pages
    :param search: the search query string, if any
    :param status: 'all', 'active', or 'paused'
    :param tags: array of strings of the current filtered tags
    :param window: the number of pages to be shown in the paging component (7 default)
    :param sorting_key: the sorting key selected for dags, None indicates that sorting is not needed/provided
    :param sorting_direction: direction of sorting, 'asc' or 'desc',
    None indicates that sorting is not needed/provided
    :return: the HTML string of the paging component
    """
    void_link = "javascript:void(0)"
    first_node = Markup(
        """<li class="paginate_button {disabled}" id="dags_first">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="0" tabindex="0">&laquo;</a>
</li>"""
    )

    previous_node = Markup(
        """<li class="paginate_button previous {disabled}" id="dags_previous">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="0" tabindex="0">&lsaquo;</a>
</li>"""
    )

    next_node = Markup(
        """<li class="paginate_button next {disabled}" id="dags_next">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="3" tabindex="0">&rsaquo;</a>
</li>"""
    )

    last_node = Markup(
        """<li class="paginate_button {disabled}" id="dags_last">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="3" tabindex="0">&raquo;</a>
</li>"""
    )

    page_node = Markup(
        """<li class="paginate_button {is_active}">
    <a href="{href_link}" aria-controls="dags" data-dt-idx="2" tabindex="0">{page_num}</a>
</li>"""
    )

    output = [Markup('<ul class="pagination" style="margin-top:0;">')]

    is_disabled = "disabled" if current_page <= 0 else ""

    qs = get_params(
        page=0,
        search=search,
        status=status,
        tags=tags,
        sorting_key=sorting_key,
        sorting_direction=sorting_direction,
    )
    first_node_link = void_link if is_disabled else f"?{qs}"
    output.append(
        first_node.format(
            href_link=first_node_link,
            disabled=is_disabled,
        )
    )

    page_link = void_link
    if current_page > 0:
        qs = get_params(
            page=current_page - 1,
            search=search,
            status=status,
            tags=tags,
            sorting_key=sorting_key,
            sorting_direction=sorting_direction,
        )
        page_link = f"?{qs}"

    output.append(previous_node.format(href_link=page_link, disabled=is_disabled))

    mid = window // 2
    last_page = num_of_pages - 1

    if current_page <= mid or num_of_pages < window:
        pages = list(range(0, min(num_of_pages, window)))
    elif mid < current_page < last_page - mid:
        pages = list(range(current_page - mid, current_page + mid + 1))
    else:
        pages = list(range(num_of_pages - window, last_page + 1))

    def is_current(current, page):
        return page == current

    for page in pages:
        qs = get_params(
            page=page,
            search=search,
            status=status,
            tags=tags,
            sorting_key=sorting_key,
            sorting_direction=sorting_direction,
        )
        vals = {
            "is_active": "active" if is_current(current_page, page) else "",
            "href_link": void_link if is_current(current_page, page) else f"?{qs}",
            "page_num": page + 1,
        }
        output.append(page_node.format(**vals))

    is_disabled = "disabled" if current_page >= num_of_pages - 1 else ""

    qs = get_params(
        page=current_page + 1,
        search=search,
        status=status,
        tags=tags,
        sorting_key=sorting_key,
        sorting_direction=sorting_direction,
    )
    page_link = void_link if current_page >= num_of_pages - 1 else f"?{qs}"

    output.append(next_node.format(href_link=page_link, disabled=is_disabled))

    qs = get_params(
        page=last_page,
        search=search,
        status=status,
        tags=tags,
        sorting_key=sorting_key,
        sorting_direction=sorting_direction,
    )
    last_node_link = void_link if is_disabled else f"?{qs}"
    output.append(
        last_node.format(
            href_link=last_node_link,
            disabled=is_disabled,
        )
    )

    output.append(Markup("</ul>"))

    return Markup("\n".join(output))


def epoch(dttm):
    """Return an epoch-type date (tuple with no timezone)."""
    return (int(time.mktime(dttm.timetuple())) * 1000,)


def make_cache_key(*args, **kwargs):
    """Get a unique key per URL; used by cache."""
    path = request.path
    args = str(hash(frozenset(request.args.items())))
    return (path + args).encode("ascii", "ignore")


def task_instance_link(attr):
    """Generate a URL to the Graph view for a TaskInstance."""
    dag_id = attr.get("dag_id")
    task_id = attr.get("task_id")
    run_id = attr.get("run_id")
    map_index = attr.get("map_index", None)
    execution_date = attr.get("execution_date") or attr.get("dag_run.execution_date")

    if map_index == -1:
        map_index = None

    url = url_for(
        "Airflow.grid",
        dag_id=dag_id,
        task_id=task_id,
        dag_run_id=run_id,
        map_index=map_index,
        execution_date=execution_date,
        tab="graph",
    )
    url_root = url_for(
        "Airflow.grid",
        dag_id=dag_id,
        task_id=task_id,
        root=task_id,
        dag_run_id=run_id,
        map_index=map_index,
        execution_date=execution_date,
        tab="graph",
    )
    return Markup(
        """
        <span style="white-space: nowrap;">
        <a href="{url}">{task_id}</a>
        <a href="{url_root}" title="Filter on this task">
        <span class="material-icons" style="margin-left:0;"
            aria-hidden="true">filter_alt</span>
        </a>
        </span>
        """
    ).format(url=url, task_id=task_id, url_root=url_root)


def state_token(state):
    """Return a formatted string with HTML for a given State."""
    color = State.color(state)
    fg_color = State.color_fg(state)
    return Markup(
        """
        <span class="label" style="color:{fg_color}; background-color:{color};"
            title="Current State: {state}">{state}</span>
        """
    ).format(color=color, state=state, fg_color=fg_color)


def state_f(attr):
    """Get 'state' & return a formatted string with HTML for a given State."""
    state = attr.get("state")
    return state_token(state)


def nobr_f(attr_name):
    """Return a formatted string with HTML with a Non-breaking Text element."""

    def nobr(attr):
        f = attr.get(attr_name)
        return Markup("<nobr>{}</nobr>").format(f)

    return nobr


def datetime_f(attr_name):
    """Return a formatted string with HTML for given DataTime."""

    def dt(attr):
        f = attr.get(attr_name)
        return datetime_html(f)

    return dt


def datetime_html(dttm: DateTime | None) -> str:
    """Return an HTML formatted string with time element to support timezone changes in UI."""
    as_iso = dttm.isoformat() if dttm else ""
    if not as_iso:
        return Markup("")
    as_iso_short = as_iso
    if timezone.utcnow().isoformat()[:4] == as_iso[:4]:
        as_iso_short = as_iso[5:]
    # The empty title will be replaced in JS code when non-UTC dates are displayed
    return Markup('<nobr><time title="" datetime="{}">{}</time></nobr>').format(as_iso, as_iso_short)


def json_f(attr_name):
    """Return a formatted string with HTML for given JSON serializable."""

    def json_(attr):
        f = attr.get(attr_name)
        serialized = json.dumps(f, cls=WebEncoder)
        return Markup("<nobr>{}</nobr>").format(serialized)

    return json_


def dag_link(attr):
    """Generate a URL to the Graph view for a Dag."""
    dag_id = attr.get("dag_id")
    execution_date = attr.get("execution_date") or attr.get("dag_run.execution_date")
    if not dag_id:
        return Markup("None")
    url = url_for("Airflow.grid", dag_id=dag_id, execution_date=execution_date)
    return Markup('<a href="{}">{}</a>').format(url, dag_id)


def dag_run_link(attr):
    """Generate a URL to the Graph view for a DagRun."""
    dag_id = attr.get("dag_id")
    run_id = attr.get("run_id")
    execution_date = attr.get("execution_date") or attr.get("dag_run.execution_date")

    if not dag_id:
        return Markup("None")

    url = url_for(
        "Airflow.grid",
        dag_id=dag_id,
        execution_date=execution_date,
        dag_run_id=run_id,
        tab="graph",
    )
    return Markup('<a href="{url}">{run_id}</a>').format(url=url, run_id=run_id)


def _get_run_ordering_expr(name: str) -> ColumnOperators:
    expr = DagRun.__mapper__.columns[name]
    # Data interval columns are NULL for runs created before 2.3, but SQL's
    # NULL-sorting logic would make those old runs always appear first. In a
    # perfect world we'd want to sort by ``get_run_data_interval()``, but that's
    # not efficient, so instead the columns are coalesced into execution_date,
    # which is good enough in most cases.
    if name in ("data_interval_start", "data_interval_end"):
        expr = func.coalesce(expr, DagRun.execution_date)
    return expr.desc()


def sorted_dag_runs(
    query: Select, *, ordering: Sequence[str], limit: int, session: Session
) -> Sequence[DagRun]:
    """
    Produce DAG runs sorted by specified columns.

    :param query: An ORM select object against *DagRun*.
    :param ordering: Column names to sort the runs. should generally come from a
        timetable's ``run_ordering``.
    :param limit: Number of runs to limit to.
    :param session: SQLAlchemy ORM session object
    :return: A list of DagRun objects ordered by the specified columns. The list
        contains only the *last* objects, but in *ascending* order.
    """
    ordering_exprs = (_get_run_ordering_expr(name) for name in ordering)
    runs = session.scalars(query.order_by(*ordering_exprs, DagRun.id.desc()).limit(limit)).all()
    runs.reverse()
    return runs


def format_map_index(attr: dict) -> str:
    """Format map index for list columns in model view."""
    value = attr["map_index"]
    if value < 0:
        return Markup("&nbsp;")
    return str(value)


def pygment_html_render(s, lexer=lexers.TextLexer):
    """Highlight text using a given Lexer."""
    return highlight(s, lexer(), HtmlFormatter(linenos=True))


def render(obj: Any, lexer: Lexer, handler: Callable[[Any], str] | None = None):
    """Render a given Python object with a given Pygments lexer."""
    if isinstance(obj, str):
        return Markup(pygment_html_render(obj, lexer))

    elif isinstance(obj, (tuple, list)):
        out = ""
        for i, text_to_render in enumerate(obj):
            if lexer is lexers.PythonLexer:
                text_to_render = repr(text_to_render)
            out += Markup("<div>List item #{}</div>").format(i)
            out += Markup("<div>" + pygment_html_render(text_to_render, lexer) + "</div>")
        return out

    elif isinstance(obj, dict):
        out = ""
        for k, v in obj.items():
            if lexer is lexers.PythonLexer:
                v = repr(v)
            out += Markup('<div>Dict item "{}"</div>').format(k)
            out += Markup("<div>" + pygment_html_render(v, lexer) + "</div>")
        return out

    elif handler is not None and obj is not None:
        return Markup(pygment_html_render(handler(obj), lexer))

    else:
        # Return empty string otherwise
        return ""


def json_render(obj, lexer):
    """Render a given Python object with json lexer."""
    out = ""
    if isinstance(obj, str):
        out = Markup(pygment_html_render(obj, lexer))
    elif isinstance(obj, (dict, list)):
        content = json.dumps(obj, sort_keys=True, indent=4)
        out = Markup(pygment_html_render(content, lexer))
    return out


def wrapped_markdown(s, css_class="rich_doc"):
    """Convert a Markdown string to HTML."""
    md = MarkdownIt("gfm-like", {"html": False})
    if s is None:
        return None
    s = textwrap.dedent(s)
    return Markup(f'<div class="{css_class}" >{md.render(s)}</div>')


def get_attr_renderer():
    """Return Dictionary containing different Pygments Lexers for Rendering & Highlighting."""
    return {
        "bash": lambda x: render(x, lexers.BashLexer),
        "bash_command": lambda x: render(x, lexers.BashLexer),
        "doc": lambda x: render(x, lexers.TextLexer),
        "doc_json": lambda x: render(x, lexers.JsonLexer),
        "doc_md": wrapped_markdown,
        "doc_rst": lambda x: render(x, lexers.RstLexer),
        "doc_yaml": lambda x: render(x, lexers.YamlLexer),
        "hql": lambda x: render(x, lexers.SqlLexer),
        "html": lambda x: render(x, lexers.HtmlLexer),
        "jinja": lambda x: render(x, lexers.DjangoLexer),
        "json": lambda x: json_render(x, lexers.JsonLexer),
        "md": wrapped_markdown,
        "mysql": lambda x: render(x, lexers.MySqlLexer),
        "postgresql": lambda x: render(x, lexers.PostgresLexer),
        "powershell": lambda x: render(x, lexers.PowerShellLexer),
        "py": lambda x: render(x, lexers.PythonLexer, get_python_source),
        "python_callable": lambda x: render(x, lexers.PythonLexer, get_python_source),
        "rst": lambda x: render(x, lexers.RstLexer),
        "sql": lambda x: render(x, lexers.SqlLexer),
        "tsql": lambda x: render(x, lexers.TransactSqlLexer),
        "yaml": lambda x: render(x, lexers.YamlLexer),
    }


class UtcAwareFilterMixin:
    """Mixin for filter for UTC time."""

    def apply(self, query, value):
        """Apply the filter."""
        if isinstance(value, str) and not value.strip():
            value = None
        else:
            value = timezone.parse(value, timezone=timezone.utc)

        return super().apply(query, value)


class FilterIsNull(BaseFilter):
    """Is null filter."""

    name = lazy_gettext("Is Null")
    arg_name = "emp"

    def apply(self, query, value):
        query, field = get_field_setup_query(query, self.model, self.column_name)
        value = set_value_to_type(self.datamodel, self.column_name, None)
        return query.filter(field == value)


class FilterIsNotNull(BaseFilter):
    """Is not null filter."""

    name = lazy_gettext("Is not Null")
    arg_name = "nemp"

    def apply(self, query, value):
        query, field = get_field_setup_query(query, self.model, self.column_name)
        value = set_value_to_type(self.datamodel, self.column_name, None)
        return query.filter(field != value)


class FilterGreaterOrEqual(BaseFilter):
    """Greater than or Equal filter."""

    name = lazy_gettext("Greater than or Equal")
    arg_name = "gte"

    def apply(self, query, value):
        query, field = get_field_setup_query(query, self.model, self.column_name)
        value = set_value_to_type(self.datamodel, self.column_name, value)

        if value is None:
            return query

        return query.filter(field >= value)


class FilterSmallerOrEqual(BaseFilter):
    """Smaller than or Equal filter."""

    name = lazy_gettext("Smaller than or Equal")
    arg_name = "lte"

    def apply(self, query, value):
        query, field = get_field_setup_query(query, self.model, self.column_name)
        value = set_value_to_type(self.datamodel, self.column_name, value)

        if value is None:
            return query

        return query.filter(field <= value)


class UtcAwareFilterSmallerOrEqual(UtcAwareFilterMixin, FilterSmallerOrEqual):
    """Smaller than or Equal filter for UTC time."""


class UtcAwareFilterGreaterOrEqual(UtcAwareFilterMixin, FilterGreaterOrEqual):
    """Greater than or Equal filter for UTC time."""


class UtcAwareFilterEqual(UtcAwareFilterMixin, fab_sqlafilters.FilterEqual):
    """Equality filter for UTC time."""


class UtcAwareFilterGreater(UtcAwareFilterMixin, fab_sqlafilters.FilterGreater):
    """Greater Than filter for UTC time."""


class UtcAwareFilterSmaller(UtcAwareFilterMixin, fab_sqlafilters.FilterSmaller):
    """Smaller Than filter for UTC time."""


class UtcAwareFilterNotEqual(UtcAwareFilterMixin, fab_sqlafilters.FilterNotEqual):
    """Not Equal To filter for UTC time."""


class UtcAwareFilterConverter(fab_sqlafilters.SQLAFilterConverter):
    """Retrieve conversion tables for UTC-Aware filters."""


class AirflowFilterConverter(fab_sqlafilters.SQLAFilterConverter):
    """Retrieve conversion tables for Airflow-specific filters."""

    conversion_table = (
        (
            "is_utcdatetime",
            [
                UtcAwareFilterEqual,
                UtcAwareFilterGreater,
                UtcAwareFilterSmaller,
                UtcAwareFilterNotEqual,
                UtcAwareFilterSmallerOrEqual,
                UtcAwareFilterGreaterOrEqual,
            ],
        ),
        # FAB will try to create filters for extendedjson fields even though we
        # exclude them from all UI, so we add this here to make it ignore them.
        (
            "is_extendedjson",
            [],
        ),
        *fab_sqlafilters.SQLAFilterConverter.conversion_table,
    )

    def __init__(self, datamodel):
        super().__init__(datamodel)

        for _, filters in self.conversion_table:
            if FilterIsNull not in filters:
                filters.append(FilterIsNull)
            if FilterIsNotNull not in filters:
                filters.append(FilterIsNotNull)


class CustomSQLAInterface(SQLAInterface):
    """
    FAB does not know how to handle columns with leading underscores because they are not supported by WTForm.

    This hack will remove the leading '_' from the key to lookup the column names.
    """

    def __init__(self, obj, session: Session | None = None):
        super().__init__(obj, session=session)

        def clean_column_names():
            if self.list_properties:
                self.list_properties = {k.lstrip("_"): v for k, v in self.list_properties.items()}
            if self.list_columns:
                self.list_columns = {k.lstrip("_"): v for k, v in self.list_columns.items()}

        clean_column_names()
        # Support for AssociationProxy in search and list columns
        for obj_attr, desc in self.obj.__mapper__.all_orm_descriptors.items():
            if isinstance(desc, AssociationProxy):
                proxy_instance = getattr(self.obj, obj_attr)
                if hasattr(proxy_instance.remote_attr.prop, "columns"):
                    self.list_columns[obj_attr] = proxy_instance.remote_attr.prop.columns[0]
                    self.list_properties[obj_attr] = proxy_instance.remote_attr.prop

    def is_utcdatetime(self, col_name):
        """Check if the datetime is a UTC one."""
        from airflow.utils.sqlalchemy import UtcDateTime

        if col_name in self.list_columns:
            obj = self.list_columns[col_name].type
            return (
                isinstance(obj, UtcDateTime)
                or isinstance(obj, types.TypeDecorator)
                and isinstance(obj.impl, UtcDateTime)
            )
        return False

    def is_extendedjson(self, col_name):
        """Check if it is a special extended JSON type."""
        from airflow.utils.sqlalchemy import ExtendedJSON

        if col_name in self.list_columns:
            obj = self.list_columns[col_name].type
            return (
                isinstance(obj, ExtendedJSON)
                or isinstance(obj, types.TypeDecorator)
                and isinstance(obj.impl, ExtendedJSON)
            )
        return False

    def get_col_default(self, col_name: str) -> Any:
        if col_name not in self.list_columns:
            # Handle AssociationProxy etc, or anything that isn't a "real" column
            return None
        return super().get_col_default(col_name)

    filter_converter_class = AirflowFilterConverter


class DagRunCustomSQLAInterface(CustomSQLAInterface):
    """
    Custom interface to allow faster deletion.

    The ``delete`` and ``delete_all`` methods are overridden to speed up
    deletion when a DAG run has a lot of related task instances. Relying on
    SQLAlchemy's cascading deletion is comparatively slow in this situation.
    """

    def delete(self, item: Model, raise_exception: bool = False) -> bool:
        self.session.execute(delete(TI).where(TI.dag_id == item.dag_id, TI.run_id == item.run_id))
        return super().delete(item, raise_exception=raise_exception)

    def delete_all(self, items: list[Model]) -> bool:
        self.session.execute(
            delete(TI).where(
                tuple_in_condition(
                    (TI.dag_id, TI.run_id),
                    ((x.dag_id, x.run_id) for x in items),
                )
            )
        )
        return super().delete_all(items)


# This class is used directly (i.e. we can't tell Fab to use a different
# subclass) so we have no other option than to edit the conversion table in
# place
FieldConverter.conversion_table = (
    ("is_utcdatetime", DateTimeWithTimezoneField, AirflowDateTimePickerWidget),
    *FieldConverter.conversion_table,
)


class UIAlert:
    """
    Helper for alerts messages shown on the UI.

    :param message: The message to display, either a string or Markup
    :param category: The category of the message, one of "info", "warning", "error", or any custom category.
        Defaults to "info".
    :param roles: List of roles that should be shown the message. If ``None``, show to all users.
    :param html: Whether the message has safe html markup in it. Defaults to False.


    For example, show a message to all users:

    .. code-block:: python

        UIAlert("Welcome to Airflow")

    Or only for users with the User role:

    .. code-block:: python

        UIAlert("Airflow update happening next week", roles=["User"])

    You can also pass html in the message:

    .. code-block:: python

        UIAlert('Visit <a href="https://airflow.apache.org">airflow.apache.org</a>', html=True)

        # or safely escape part of the message
        # (more details: https://markupsafe.palletsprojects.com/en/2.0.x/formatting/)
        UIAlert(Markup("Welcome <em>%s</em>") % ("John & Jane Doe",))
    """

    def __init__(
        self,
        message: str | Markup,
        category: str = "info",
        roles: list[str] | None = None,
        html: bool = False,
    ):
        self.category = category
        self.roles = roles
        self.html = html
        self.message = Markup(message) if html else message

    def should_show(self) -> bool:
        """
        Determine if the user should see the message.

        The decision is based on the user's role.
        """
        if self.roles:
            current_user = get_auth_manager().get_user()
            if current_user is not None:
                user_roles = {r.name for r in getattr(current_user, "roles", [])}
            else:
                # Unable to obtain user role - default to not showing
                return False

            if user_roles.isdisjoint(self.roles):
                return False
        return True
