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
import textwrap
import time
from typing import Any
from urllib.parse import urlencode

import sqlalchemy as sqla
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
from pendulum.datetime import DateTime
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter
from sqlalchemy.ext.associationproxy import AssociationProxy

from airflow import models
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.models import errors
from airflow.models.dagwarning import DagWarning
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.code_utils import get_python_source
from airflow.utils.helpers import alchemy_to_dict
from airflow.utils.state import State, TaskInstanceState
from airflow.www.forms import DateTimeWithTimezoneField
from airflow.www.widgets import AirflowDateTimePickerWidget


def datetime_to_string(value: DateTime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def get_mapped_instances(task_instance, session):
    return (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == task_instance.dag_id,
            TaskInstance.run_id == task_instance.run_id,
            TaskInstance.task_id == task_instance.task_id,
        )
        .order_by(TaskInstance.map_index)
        .all()
    )


def get_instance_with_map(task_instance, session):
    if task_instance.map_index == -1:
        return alchemy_to_dict(task_instance)
    mapped_instances = get_mapped_instances(task_instance, session)
    return get_mapped_summary(task_instance, mapped_instances)


priority = [
    TaskInstanceState.FAILED,
    TaskInstanceState.UPSTREAM_FAILED,
    TaskInstanceState.UP_FOR_RETRY,
    TaskInstanceState.UP_FOR_RESCHEDULE,
    TaskInstanceState.QUEUED,
    TaskInstanceState.SCHEDULED,
    TaskInstanceState.DEFERRED,
    TaskInstanceState.RUNNING,
    TaskInstanceState.SHUTDOWN,
    TaskInstanceState.RESTARTING,
    TaskInstanceState.REMOVED,
    None,
    TaskInstanceState.SUCCESS,
    TaskInstanceState.SKIPPED,
]


def get_mapped_summary(parent_instance, task_instances):
    mapped_states = [ti.state for ti in task_instances]

    group_state = None
    for state in priority:
        if state in mapped_states:
            group_state = state
            break

    group_start_date = datetime_to_string(
        min((ti.start_date for ti in task_instances if ti.start_date), default=None)
    )
    group_end_date = datetime_to_string(
        max((ti.end_date for ti in task_instances if ti.end_date), default=None)
    )

    try_count = (
        parent_instance._try_number
        if parent_instance._try_number != 0 or parent_instance.state in State.running
        else parent_instance._try_number + 1
    )

    return {
        'task_id': parent_instance.task_id,
        'run_id': parent_instance.run_id,
        'state': group_state,
        'start_date': group_start_date,
        'end_date': group_end_date,
        'mapped_states': mapped_states,
        'try_number': try_count,
    }


def encode_dag_run(dag_run: models.DagRun | None) -> dict[str, Any] | None:
    if not dag_run:
        return None

    return {
        'run_id': dag_run.run_id,
        'start_date': datetime_to_string(dag_run.start_date),
        'end_date': datetime_to_string(dag_run.end_date),
        'state': dag_run.state,
        'execution_date': datetime_to_string(dag_run.execution_date),
        'data_interval_start': datetime_to_string(dag_run.data_interval_start),
        'data_interval_end': datetime_to_string(dag_run.data_interval_end),
        'run_type': dag_run.run_type,
        'last_scheduling_decision': datetime_to_string(dag_run.last_scheduling_decision),
    }


def check_import_errors(fileloc, session):
    # Check dag import errors
    import_errors = session.query(errors.ImportError).filter(errors.ImportError.filename == fileloc).all()
    if import_errors:
        for import_error in import_errors:
            flash("Broken DAG: [{ie.filename}] {ie.stacktrace}".format(ie=import_error), "dag_import_error")


def check_dag_warnings(dag_id, session):
    dag_warnings = session.query(DagWarning).filter(DagWarning.dag_id == dag_id).all()
    if dag_warnings:
        for dag_warning in dag_warnings:
            flash(dag_warning.message, "warning")


def get_sensitive_variables_fields():
    import warnings

    from airflow.utils.log.secrets_masker import get_sensitive_variables_fields

    warnings.warn(
        "This function is deprecated. Please use "
        "`airflow.utils.log.secrets_masker.get_sensitive_variables_fields`",
        RemovedInAirflow3Warning,
        stacklevel=2,
    )
    return get_sensitive_variables_fields()


def should_hide_value_for_key(key_name):
    import warnings

    from airflow.utils.log.secrets_masker import should_hide_value_for_key

    warnings.warn(
        "This function is deprecated. Please use "
        "`airflow.utils.log.secrets_masker.should_hide_value_for_key`",
        RemovedInAirflow3Warning,
        stacklevel=2,
    )
    return should_hide_value_for_key(key_name)


def get_params(**kwargs):
    """Return URL-encoded params"""
    return urlencode({d: v for d, v in kwargs.items() if v is not None}, True)


def generate_pages(current_page, num_of_pages, search=None, status=None, tags=None, window=7):
    """
    Generates the HTML for a paging component using a similar logic to the paging
    auto-generated by Flask managed views. The paging component defines a number of
    pages visible in the pager (window) and once the user goes to a page beyond the
    largest visible, it would scroll to the right the page numbers and keeps the
    current one in the middle of the pager component. When in the last pages,
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
    :return: the HTML string of the paging component
    """
    void_link = 'javascript:void(0)'
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

    is_disabled = 'disabled' if current_page <= 0 else ''

    first_node_link = (
        void_link if is_disabled else f'?{get_params(page=0, search=search, status=status, tags=tags)}'
    )
    output.append(
        first_node.format(
            href_link=first_node_link,
            disabled=is_disabled,
        )
    )

    page_link = void_link
    if current_page > 0:
        page_link = f'?{get_params(page=current_page - 1, search=search, status=status, tags=tags)}'

    output.append(previous_node.format(href_link=page_link, disabled=is_disabled))

    mid = int(window / 2)
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
        vals = {
            'is_active': 'active' if is_current(current_page, page) else '',
            'href_link': void_link
            if is_current(current_page, page)
            else f'?{get_params(page=page, search=search, status=status, tags=tags)}',
            'page_num': page + 1,
        }
        output.append(page_node.format(**vals))

    is_disabled = 'disabled' if current_page >= num_of_pages - 1 else ''

    page_link = (
        void_link
        if current_page >= num_of_pages - 1
        else f'?{get_params(page=current_page + 1, search=search, status=status, tags=tags)}'
    )

    output.append(next_node.format(href_link=page_link, disabled=is_disabled))

    last_node_link = (
        void_link
        if is_disabled
        else f'?{get_params(page=last_page, search=search, status=status, tags=tags)}'
    )
    output.append(
        last_node.format(
            href_link=last_node_link,
            disabled=is_disabled,
        )
    )

    output.append(Markup('</ul>'))

    return Markup('\n'.join(output))


def epoch(dttm):
    """Returns an epoch-type date (tuple with no timezone)"""
    return (int(time.mktime(dttm.timetuple())) * 1000,)


def make_cache_key(*args, **kwargs):
    """Used by cache to get a unique key per URL"""
    path = request.path
    args = str(hash(frozenset(request.args.items())))
    return (path + args).encode('ascii', 'ignore')


def task_instance_link(attr):
    """Generates a URL to the Graph view for a TaskInstance."""
    dag_id = attr.get('dag_id')
    task_id = attr.get('task_id')
    execution_date = attr.get('dag_run.execution_date') or attr.get('execution_date') or timezone.utcnow()
    url = url_for(
        'Airflow.task',
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date.isoformat(),
        map_index=attr.get('map_index', -1),
    )
    url_root = url_for(
        'Airflow.graph', dag_id=dag_id, root=task_id, execution_date=execution_date.isoformat()
    )
    return Markup(
        """
        <span style="white-space: nowrap;">
        <a href="{url}">{task_id}</a>
        <a href="{url_root}" title="Filter on this task and upstream">
        <span class="material-icons" style="margin-left:0;"
            aria-hidden="true">filter_alt</span>
        </a>
        </span>
        """
    ).format(url=url, task_id=task_id, url_root=url_root)


def state_token(state):
    """Returns a formatted string with HTML for a given State"""
    color = State.color(state)
    fg_color = State.color_fg(state)
    return Markup(
        """
        <span class="label" style="color:{fg_color}; background-color:{color};"
            title="Current State: {state}">{state}</span>
        """
    ).format(color=color, state=state, fg_color=fg_color)


def state_f(attr):
    """Gets 'state' & returns a formatted string with HTML for a given State"""
    state = attr.get('state')
    return state_token(state)


def nobr_f(attr_name):
    """Returns a formatted string with HTML with a Non-breaking Text element"""

    def nobr(attr):
        f = attr.get(attr_name)
        return Markup("<nobr>{}</nobr>").format(f)

    return nobr


def datetime_f(attr_name):
    """Returns a formatted string with HTML for given DataTime"""

    def dt(attr):
        f = attr.get(attr_name)
        return datetime_html(f)

    return dt


def datetime_html(dttm: DateTime | None) -> str:
    """Return an HTML formatted string with time element to support timezone changes in UI"""
    as_iso = dttm.isoformat() if dttm else ''
    if not as_iso:
        return Markup('')
    as_iso_short = as_iso
    if timezone.utcnow().isoformat()[:4] == as_iso[:4]:
        as_iso_short = as_iso[5:]
    # The empty title will be replaced in JS code when non-UTC dates are displayed
    return Markup('<nobr><time title="" datetime="{}">{}</time></nobr>').format(as_iso, as_iso_short)


def json_f(attr_name):
    """Returns a formatted string with HTML for given JSON serializable"""

    def json_(attr):
        f = attr.get(attr_name)
        serialized = json.dumps(f)
        return Markup('<nobr>{}</nobr>').format(serialized)

    return json_


def dag_link(attr):
    """Generates a URL to the Graph view for a Dag."""
    dag_id = attr.get('dag_id')
    execution_date = attr.get('execution_date')
    if not dag_id:
        return Markup('None')
    url = url_for('Airflow.graph', dag_id=dag_id, execution_date=execution_date)
    return Markup('<a href="{}">{}</a>').format(url, dag_id)


def dag_run_link(attr):
    """Generates a URL to the Graph view for a DagRun."""
    dag_id = attr.get('dag_id')
    run_id = attr.get('run_id')
    execution_date = attr.get('dag_run.exectuion_date') or attr.get('execution_date')
    url = url_for('Airflow.graph', dag_id=dag_id, run_id=run_id, execution_date=execution_date)
    return Markup('<a href="{url}">{run_id}</a>').format(url=url, run_id=run_id)


def format_map_index(attr: dict) -> str:
    """Format map index for list columns in model view."""
    value = attr['map_index']
    if value < 0:
        return Markup("&nbsp;")
    return str(value)


def pygment_html_render(s, lexer=lexers.TextLexer):
    """Highlight text using a given Lexer"""
    return highlight(s, lexer(), HtmlFormatter(linenos=True))


def render(obj, lexer):
    """Render a given Python object with a given Pygments lexer"""
    out = ""
    if isinstance(obj, str):
        out = Markup(pygment_html_render(obj, lexer))
    elif isinstance(obj, (tuple, list)):
        for i, text_to_render in enumerate(obj):
            out += Markup("<div>List item #{}</div>").format(i)
            out += Markup("<div>" + pygment_html_render(text_to_render, lexer) + "</div>")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            out += Markup('<div>Dict item "{}"</div>').format(k)
            out += Markup("<div>" + pygment_html_render(v, lexer) + "</div>")
    return out


def json_render(obj, lexer):
    """Render a given Python object with json lexer"""
    out = ""
    if isinstance(obj, str):
        out = Markup(pygment_html_render(obj, lexer))
    elif isinstance(obj, (dict, list)):
        content = json.dumps(obj, sort_keys=True, indent=4)
        out = Markup(pygment_html_render(content, lexer))
    return out


def wrapped_markdown(s, css_class='rich_doc'):
    """Convert a Markdown string to HTML."""
    md = MarkdownIt("gfm-like")
    if s is None:
        return None
    s = textwrap.dedent(s)
    return Markup(f'<div class="{css_class}" >{md.render(s)}</div>')


def get_attr_renderer():
    """Return Dictionary containing different Pygments Lexers for Rendering & Highlighting"""
    return {
        'bash': lambda x: render(x, lexers.BashLexer),
        'bash_command': lambda x: render(x, lexers.BashLexer),
        'doc': lambda x: render(x, lexers.TextLexer),
        'doc_json': lambda x: render(x, lexers.JsonLexer),
        'doc_md': wrapped_markdown,
        'doc_rst': lambda x: render(x, lexers.RstLexer),
        'doc_yaml': lambda x: render(x, lexers.YamlLexer),
        'hql': lambda x: render(x, lexers.SqlLexer),
        'html': lambda x: render(x, lexers.HtmlLexer),
        'jinja': lambda x: render(x, lexers.DjangoLexer),
        'json': lambda x: json_render(x, lexers.JsonLexer),
        'md': wrapped_markdown,
        'mysql': lambda x: render(x, lexers.MySqlLexer),
        'postgresql': lambda x: render(x, lexers.PostgresLexer),
        'powershell': lambda x: render(x, lexers.PowerShellLexer),
        'py': lambda x: render(get_python_source(x), lexers.PythonLexer),
        'python_callable': lambda x: render(get_python_source(x), lexers.PythonLexer),
        'rst': lambda x: render(x, lexers.RstLexer),
        'sql': lambda x: render(x, lexers.SqlLexer),
        'tsql': lambda x: render(x, lexers.TransactSqlLexer),
        'yaml': lambda x: render(x, lexers.YamlLexer),
    }


def get_chart_height(dag):
    """
    We use the number of tasks in the DAG as a heuristic to
    approximate the size of generated chart (otherwise the charts are tiny and unreadable
    when DAGs have a large number of tasks). Ideally nvd3 should allow for dynamic-height
    charts, that is charts that take up space based on the size of the components within.
    TODO(aoen): See [AIRFLOW-1263]
    """
    return 600 + len(dag.tasks) * 10


class UtcAwareFilterMixin:
    """Mixin for filter for UTC time."""

    def apply(self, query, value):
        """Apply the filter."""
        value = timezone.parse(value, timezone=timezone.utc)

        return super().apply(query, value)


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
            'is_utcdatetime',
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
            'is_extendedjson',
            [],
        ),
    ) + fab_sqlafilters.SQLAFilterConverter.conversion_table


class CustomSQLAInterface(SQLAInterface):
    """
    FAB does not know how to handle columns with leading underscores because
    they are not supported by WTForm. This hack will remove the leading
    '_' from the key to lookup the column names.

    """

    def __init__(self, obj, session=None):
        super().__init__(obj, session=session)

        def clean_column_names():
            if self.list_properties:
                self.list_properties = {k.lstrip('_'): v for k, v in self.list_properties.items()}
            if self.list_columns:
                self.list_columns = {k.lstrip('_'): v for k, v in self.list_columns.items()}

        clean_column_names()
        # Support for AssociationProxy in search and list columns
        for desc in self.obj.__mapper__.all_orm_descriptors:
            if not isinstance(desc, AssociationProxy):
                continue
            proxy_instance = getattr(self.obj, desc.value_attr)
            if hasattr(proxy_instance.remote_attr.prop, 'columns'):
                self.list_columns[desc.value_attr] = proxy_instance.remote_attr.prop.columns[0]
                self.list_properties[desc.value_attr] = proxy_instance.remote_attr.prop

    def is_utcdatetime(self, col_name):
        """Check if the datetime is a UTC one."""
        from airflow.utils.sqlalchemy import UtcDateTime

        if col_name in self.list_columns:
            obj = self.list_columns[col_name].type
            return (
                isinstance(obj, UtcDateTime)
                or isinstance(obj, sqla.types.TypeDecorator)
                and isinstance(obj.impl, UtcDateTime)
            )
        return False

    def is_extendedjson(self, col_name):
        """Checks if it is a special extended JSON type"""
        from airflow.utils.sqlalchemy import ExtendedJSON

        if col_name in self.list_columns:
            obj = self.list_columns[col_name].type
            return (
                isinstance(obj, ExtendedJSON)
                or isinstance(obj, sqla.types.TypeDecorator)
                and isinstance(obj.impl, ExtendedJSON)
            )
        return False

    def get_col_default(self, col_name: str) -> Any:
        if col_name not in self.list_columns:
            # Handle AssociationProxy etc, or anything that isn't a "real" column
            return None
        return super().get_col_default(col_name)

    filter_converter_class = AirflowFilterConverter


# This class is used directly (i.e. we can't tell Fab to use a different
# subclass) so we have no other option than to edit the conversion table in
# place
FieldConverter.conversion_table = (
    ('is_utcdatetime', DateTimeWithTimezoneField, AirflowDateTimePickerWidget),
) + FieldConverter.conversion_table


class UIAlert:
    """
    Helper for alerts messages shown on the UI

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

    def should_show(self, securitymanager) -> bool:
        """Determine if the user should see the message based on their role membership"""
        if self.roles:
            user_roles = {r.name for r in securitymanager.current_user.roles}
            if not user_roles.intersection(set(self.roles)):
                return False
        return True
