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

import itertools
import os
import re
import time
from datetime import datetime
from unittest.mock import Mock
from urllib.parse import parse_qs

import pendulum
import pytest
from bs4 import BeautifulSoup
from flask_appbuilder.models.sqla.filters import get_field_setup_query, set_value_to_type
from flask_wtf import FlaskForm
from markupsafe import Markup
from sqlalchemy.orm import Query
from wtforms.fields import StringField, TextAreaField

from airflow.models import DagRun
from airflow.utils import json as utils_json
from airflow.www import utils
from airflow.www.utils import (
    CustomSQLAInterface,
    DagRunCustomSQLAInterface,
    encode_dag_run,
    json_f,
    wrapped_markdown,
)
from airflow.www.widgets import AirflowDateTimePickerROWidget, BS3TextAreaROWidget, BS3TextFieldROWidget
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType


class TestUtils:
    def check_generate_pages_html(
        self,
        current_page,
        total_pages,
        window=7,
        check_middle=False,
        sorting_key=None,
        sorting_direction=None,
    ):
        extra_links = 4  # first, prev, next, last
        search = "'>\"/><img src=x onerror=alert(1)>"
        if sorting_key and sorting_direction:
            html_str = utils.generate_pages(
                current_page,
                total_pages,
                search=search,
                sorting_key=sorting_key,
                sorting_direction=sorting_direction,
            )
        else:
            html_str = utils.generate_pages(current_page, total_pages, search=search)

        assert search not in html_str, "The raw search string shouldn't appear in the output"
        assert "search=%27%3E%22%2F%3E%3Cimg+src%3Dx+onerror%3Dalert%281%29%3E" in html_str

        assert callable(html_str.__html__), "Should return something that is HTML-escaping aware"

        dom = BeautifulSoup(html_str, "html.parser")
        assert dom is not None

        ulist = dom.ul
        ulist_items = ulist.find_all("li")
        assert min(window, total_pages) + extra_links == len(ulist_items)

        page_items = ulist_items[2:-2]
        mid = len(page_items) // 2
        all_nodes = []
        pages = []

        if sorting_key and sorting_direction:
            last_page = total_pages - 1

            if current_page <= mid or total_pages < window:
                pages = list(range(min(total_pages, window)))
            elif mid < current_page < last_page - mid:
                pages = list(range(current_page - mid, current_page + mid + 1))
            else:
                pages = list(range(total_pages - window, last_page + 1))

            pages.append(last_page + 1)
            pages.sort(reverse=True if sorting_direction == "desc" else False)

        for i, item in enumerate(page_items):
            a_node = item.a
            href_link = a_node["href"]
            node_text = a_node.string
            all_nodes.append(node_text)
            if node_text == str(current_page + 1):
                if check_middle:
                    assert mid == i
                assert "javascript:void(0)" == href_link
                assert "active" in item["class"]
            else:
                assert re.search(r"^\?", href_link), "Link is page-relative"
                query = parse_qs(href_link[1:])
                assert query["page"] == [str(int(node_text) - 1)]
                assert query["search"] == [search]

        if sorting_key and sorting_direction:
            if pages[0] == 0:
                pages = [str(page) for page in pages[1:]]

            assert pages == all_nodes

    def test_generate_pager_current_start(self):
        self.check_generate_pages_html(current_page=0, total_pages=6)

    def test_generate_pager_current_middle(self):
        self.check_generate_pages_html(current_page=10, total_pages=20, check_middle=True)

    def test_generate_pager_current_end(self):
        self.check_generate_pages_html(current_page=38, total_pages=39)

    def test_generate_pager_current_start_with_sorting(self):
        self.check_generate_pages_html(
            current_page=0, total_pages=4, sorting_key="dag_id", sorting_direction="asc"
        )

    def test_params_no_values(self):
        """Should return an empty string if no params are passed"""
        assert "" == utils.get_params()

    def test_params_search(self):
        assert "search=bash_" == utils.get_params(search="bash_")

    def test_params_none_and_zero(self):
        query_str = utils.get_params(a=0, b=None, c="true")
        # The order won't be consistent, but that doesn't affect behaviour of a browser
        pairs = sorted(query_str.split("&"))
        assert ["a=0", "c=true"] == pairs

    def test_params_all(self):
        query = utils.get_params(tags=["tag1", "tag2"], status="active", page=3, search="bash_")
        assert {
            "tags": ["tag1", "tag2"],
            "page": ["3"],
            "search": ["bash_"],
            "status": ["active"],
        } == parse_qs(query)

    def test_params_escape(self):
        assert "search=%27%3E%22%2F%3E%3Cimg+src%3Dx+onerror%3Dalert%281%29%3E" == utils.get_params(
            search="'>\"/><img src=x onerror=alert(1)>"
        )

    def test_state_token(self):
        # It's shouldn't possible to set these odd values anymore, but lets
        # ensure they are escaped!
        html = str(utils.state_token("<script>alert(1)</script>"))

        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
        assert "<script>alert(1)</script>" not in html

    def test_nobr_f(self):
        attr = {"attr_name": "attribute"}
        f = attr.get("attr_name")
        expected_markup = Markup("<nobr>{}</nobr>").format(f)

        nobr = utils.nobr_f("attr_name")
        result_markup = nobr(attr)

        assert result_markup == expected_markup

    def test_nobr_f_empty_attr(self):
        attr = {"attr_name": ""}
        f = attr.get("attr_name")
        expected_markup = Markup("<nobr>{}</nobr>").format(f)

        nobr = utils.nobr_f("attr_name")
        result_markup = nobr(attr)

        assert result_markup == expected_markup

    def test_nobr_f_missing_attr(self):
        attr = {}
        f = None
        expected_markup = Markup("<nobr>{}</nobr>").format(f)

        nobr = utils.nobr_f("attr_name")
        result_markup = nobr(attr)

        assert result_markup == expected_markup

    def test_epoch(self):
        test_datetime = datetime(2024, 6, 19, 12, 0, 0)
        result = utils.epoch(test_datetime)
        epoch_time = result[0]

        expected_epoch_time = int(time.mktime(test_datetime.timetuple())) * 1000

        assert epoch_time == expected_epoch_time

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.db_test
    def test_make_cache_key(self):
        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context(
            "/test/path", query_string={"key1": "value1", "key2": "value2"}
        ):
            expected_args = str(hash(frozenset({"key1": "value1", "key2": "value2"}.items())))
            expected_cache_key = ("/test/path" + expected_args).encode("ascii", "ignore")
            result_cache_key = utils.make_cache_key()
            assert result_cache_key == expected_cache_key

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.db_test
    def test_task_instance_link(self):
        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            html = str(
                utils.task_instance_link(
                    {"dag_id": "<a&1>", "task_id": "<b2>", "map_index": 1, "execution_date": datetime.now()}
                )
            )

            html_map_index_none = str(
                utils.task_instance_link(
                    {"dag_id": "<a&1>", "task_id": "<b2>", "map_index": -1, "execution_date": datetime.now()}
                )
            )

        assert "%3Ca%261%3E" in html
        assert "%3Cb2%3E" in html
        assert "map_index" in html
        assert "<a&1>" not in html
        assert "<b2>" not in html

        assert "%3Ca%261%3E" in html_map_index_none
        assert "%3Cb2%3E" in html_map_index_none
        assert "map_index" not in html_map_index_none
        assert "<a&1>" not in html_map_index_none
        assert "<b2>" not in html_map_index_none

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.db_test
    def test_dag_link(self):
        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            html = str(utils.dag_link({"dag_id": "<a&1>", "execution_date": datetime.now()}))

        assert "%3Ca%261%3E" in html
        assert "<a&1>" not in html

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.db_test
    def test_dag_link_when_dag_is_none(self):
        """Test that when there is no dag_id, dag_link does not contain hyperlink"""
        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            html = str(utils.dag_link({}))

        assert "None" in html
        assert "<a href=" not in html

    @pytest.mark.skip_if_database_isolation_mode
    @pytest.mark.db_test
    def test_dag_run_link(self):
        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            html = str(
                utils.dag_run_link({"dag_id": "<a&1>", "run_id": "<b2>", "execution_date": datetime.now()})
            )

        assert "%3Ca%261%3E" in html
        assert "%3Cb2%3E" in html
        assert "<a&1>" not in html
        assert "<b2>" not in html


class TestAttrRenderer:
    def setup_method(self):
        self.attr_renderer = utils.get_attr_renderer()

    def test_python_callable(self):
        def example_callable(unused_self):
            print("example")

        rendered = self.attr_renderer["python_callable"](example_callable)
        assert "&quot;example&quot;" in rendered

    def test_python_callable_none(self):
        rendered = self.attr_renderer["python_callable"](None)
        assert "" == rendered

    def test_markdown(self):
        markdown = "* foo\n* bar"
        rendered = self.attr_renderer["doc_md"](markdown)
        assert "<li>foo</li>" in rendered
        assert "<li>bar</li>" in rendered

    def test_markdown_none(self):
        rendered = self.attr_renderer["doc_md"](None)
        assert rendered is None

    def test_get_dag_run_conf(self):
        dag_run_conf = {
            "1": "string",
            "2": b"bytes",
            "3": 123,
            "4": "à".encode("latin"),
            "5": datetime(2023, 1, 1),
        }
        expected_encoded_dag_run_conf = (
            '{"1": "string", "2": "bytes", "3": 123, "4": "à", "5": "2023-01-01T00:00:00+00:00"}'
        )
        encoded_dag_run_conf, conf_is_json = utils.get_dag_run_conf(
            dag_run_conf, json_encoder=utils_json.WebEncoder
        )
        assert expected_encoded_dag_run_conf == encoded_dag_run_conf

    def test_encode_dag_run_none(self):
        no_dag_run_result = utils.encode_dag_run(None)
        assert no_dag_run_result == (None, None)

    def test_json_f_webencoder(self):
        dag_run_conf = {
            "1": "string",
            "2": b"bytes",
            "3": 123,
            "4": "à".encode("latin"),
            "5": datetime(2023, 1, 1),
        }
        expected_encoded_dag_run_conf = (
            # HTML sanitization is insane
            '{"1": "string", "2": "bytes", "3": 123, "4": "\\u00e0", "5": "2023-01-01T00:00:00+00:00"}'
        )
        expected_markup = Markup("<nobr>{}</nobr>").format(expected_encoded_dag_run_conf)

        formatter = json_f("conf")
        dagrun = Mock()
        dagrun.get = Mock(return_value=dag_run_conf)

        assert formatter(dagrun) == expected_markup


def create_dag_run_for_markdown():
    params = dict(run_id="run_id_1", conf={})
    if AIRFLOW_V_3_0_PLUS:
        params.update(triggered_by=DagRunTriggeredByType.TEST)
    return DagRun(**params)


class TestWrappedMarkdown:
    def test_wrapped_markdown_with_docstring_curly_braces(self):
        rendered = wrapped_markdown("{braces}", css_class="a_class")
        assert (
            """<div class="a_class" ><p>{braces}</p>
</div>"""
            == rendered
        )

    def test_wrapped_markdown_with_some_markdown(self):
        rendered = wrapped_markdown(
            """*italic*
        **bold**
        """,
            css_class="a_class",
        )

        assert (
            """<div class="a_class" ><p><em>italic</em>
<strong>bold</strong></p>
</div>"""
            == rendered
        )

    def test_wrapped_markdown_with_table(self):
        rendered = wrapped_markdown(
            """
| Job | Duration |
| ----------- | ----------- |
| ETL | 14m |
"""
        )

        assert (
            """<div class="rich_doc" ><table>
<thead>
<tr>
<th>Job</th>
<th>Duration</th>
</tr>
</thead>
<tbody>
<tr>
<td>ETL</td>
<td>14m</td>
</tr>
</tbody>
</table>
</div>"""
            == rendered
        )

    def test_wrapped_markdown_with_indented_lines(self):
        rendered = wrapped_markdown(
            """
                # header
                1st line
                2nd line
            """
        )

        assert (
            """<div class="rich_doc" ><h1>header</h1>\n<p>1st line\n2nd line</p>
</div>"""
            == rendered
        )

    def test_wrapped_markdown_with_raw_code_block(self):
        rendered = wrapped_markdown(
            """\
            # Markdown code block

            Inline `code` works well.

                Code block
                does not
                respect
                newlines

            """
        )

        assert (
            """<div class="rich_doc" ><h1>Markdown code block</h1>
<p>Inline <code>code</code> works well.</p>
<pre><code>Code block\ndoes not\nrespect\nnewlines\n</code></pre>
</div>"""
            == rendered
        )

    def test_wrapped_markdown_with_nested_list(self):
        rendered = wrapped_markdown(
            """
            ### Docstring with a code block

            - And
                - A nested list
            """
        )

        assert (
            """<div class="rich_doc" ><h3>Docstring with a code block</h3>
<ul>
<li>And
<ul>
<li>A nested list</li>
</ul>
</li>
</ul>
</div>"""
            == rendered
        )

    @pytest.mark.parametrize(
        "html",
        [
            "test <code>raw HTML</code>",
            "hidden <script>alert(1)</script> nuggets.",
        ],
    )
    def test_wrapped_markdown_with_raw_html(self, html):
        """Ensure that HTML code is not ending-up in markdown but is always escaped."""
        from markupsafe import escape

        rendered = wrapped_markdown(html)
        assert escape(html) in rendered

    @pytest.mark.parametrize(
        "dag_run,expected_val",
        [
            [None, (None, None)],
            [
                create_dag_run_for_markdown(),
                (
                    {
                        "conf": None,
                        "conf_is_json": False,
                        "data_interval_end": None,
                        "data_interval_start": None,
                        "end_date": None,
                        "execution_date": None,
                        "external_trigger": None,
                        "last_scheduling_decision": None,
                        "note": None,
                        "queued_at": None,
                        "run_id": "run_id_1",
                        "run_type": None,
                        "start_date": None,
                        "state": None,
                        "triggered_by": "test",
                    },
                    None,
                ),
            ],
        ],
    )
    def test_encode_dag_run(self, dag_run, expected_val):
        val = encode_dag_run(dag_run)
        assert val == expected_val

    def test_encode_dag_run_circular_reference(self):
        conf = {}
        conf["a"] = conf
        dr = DagRun(run_id="run_id_1", conf=conf)
        encoded_dr, error = encode_dag_run(dr)
        assert encoded_dr is None
        assert error == (
            f"Circular reference detected in the DAG Run config (#{dr.run_id}). "
            f"You should check your webserver logs for more details."
        )


class TestFilter:
    def setup_method(self):
        self.mock_datamodel = Mock()
        self.mock_query = Mock(spec=Query)
        self.mock_column_name = "test_column"

    def test_filter_is_null_apply(self):
        filter_is_null = utils.FilterIsNull(datamodel=self.mock_datamodel, column_name=self.mock_column_name)

        self.mock_query, mock_field = get_field_setup_query(
            self.mock_query, self.mock_datamodel, self.mock_column_name
        )
        mock_value = set_value_to_type(self.mock_datamodel, self.mock_column_name, None)

        result_query_filter = filter_is_null.apply(self.mock_query, None)
        self.mock_query.filter.assert_called_once_with(mock_field == mock_value)

        expected_query_filter = self.mock_query.filter(mock_field == mock_value)

        assert result_query_filter == expected_query_filter

    def test_filter_is_not_null_apply(self):
        filter_is_not_null = utils.FilterIsNotNull(
            datamodel=self.mock_datamodel, column_name=self.mock_column_name
        )

        self.mock_query, mock_field = get_field_setup_query(
            self.mock_query, self.mock_datamodel, self.mock_column_name
        )
        mock_value = set_value_to_type(self.mock_datamodel, self.mock_column_name, None)

        result_query_filter = filter_is_not_null.apply(self.mock_query, None)
        self.mock_query.filter.assert_called_once_with(mock_field != mock_value)

        expected_query_filter = self.mock_query.filter(mock_field != mock_value)

        assert result_query_filter == expected_query_filter

    def test_filter_gte_none_value_apply(self):
        filter_gte = utils.FilterGreaterOrEqual(
            datamodel=self.mock_datamodel, column_name=self.mock_column_name
        )

        self.mock_query, mock_field = get_field_setup_query(
            self.mock_query, self.mock_datamodel, self.mock_column_name
        )
        mock_value = set_value_to_type(self.mock_datamodel, self.mock_column_name, None)

        result_query_filter = filter_gte.apply(self.mock_query, mock_value)

        assert result_query_filter == self.mock_query

    def test_filter_lte_none_value_apply(self):
        filter_lte = utils.FilterSmallerOrEqual(
            datamodel=self.mock_datamodel, column_name=self.mock_column_name
        )

        self.mock_query, mock_field = get_field_setup_query(
            self.mock_query, self.mock_datamodel, self.mock_column_name
        )
        mock_value = set_value_to_type(self.mock_datamodel, self.mock_column_name, None)

        result_query_filter = filter_lte.apply(self.mock_query, mock_value)

        assert result_query_filter == self.mock_query


@pytest.mark.db_test
def test_get_col_default_not_existing(session):
    interface = CustomSQLAInterface(obj=DagRun, session=session)
    default_value = interface.get_col_default("column_not_existing")
    assert default_value is None


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
def test_dag_run_custom_sqla_interface_delete_no_collateral_damage(dag_maker, session):
    interface = DagRunCustomSQLAInterface(obj=DagRun, session=session)
    dag_ids = (f"test_dag_{x}" for x in range(1, 4))
    dates = (pendulum.datetime(2023, 1, x) for x in range(1, 4))
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    for dag_id, date in itertools.product(dag_ids, dates):
        with dag_maker(dag_id=dag_id) as dag:
            dag.create_dagrun(
                execution_date=date,
                state="running",
                run_type="scheduled",
                data_interval=(date, date),
                **triggered_by_kwargs,
            )
    dag_runs = session.query(DagRun).all()
    assert len(dag_runs) == 9
    assert len(set(x.run_id for x in dag_runs)) == 3
    run_id_for_single_delete = "scheduled__2023-01-01T00:00:00+00:00"
    # we have 3 runs with this same run_id
    assert sum(1 for x in dag_runs if x.run_id == run_id_for_single_delete) == 3
    # each is a different dag

    # if we delete one, it shouldn't delete the others
    one_run = next(x for x in dag_runs if x.run_id == run_id_for_single_delete)
    assert interface.delete(item=one_run) is True
    session.commit()
    dag_runs = session.query(DagRun).all()
    # we should have one fewer dag run now
    assert len(dag_runs) == 8

    # now let's try multi delete
    run_id_for_multi_delete = "scheduled__2023-01-02T00:00:00+00:00"
    # verify we have 3
    runs_of_interest = [x for x in dag_runs if x.run_id == run_id_for_multi_delete]
    assert len(runs_of_interest) == 3
    # and that each is different dag
    assert len(set(x.dag_id for x in dag_runs)) == 3

    to_delete = runs_of_interest[:2]
    # now try multi delete
    assert interface.delete_all(items=to_delete) is True
    session.commit()
    dag_runs = session.query(DagRun).all()
    assert len(dag_runs) == 6
    assert len(set(x.dag_id for x in dag_runs)) == 3
    assert len(set(x.run_id for x in dag_runs)) == 3


@pytest.fixture
def app():
    from flask import Flask

    app = Flask(__name__)
    app.config["WTF_CSRF_ENABLED"] = False
    app.config["SECRET_KEY"] = "secret"
    with app.app_context():
        yield app


class TestWidgets:
    def test_airflow_datetime_picker_ro_widget(self, app):
        class TestForm(FlaskForm):
            datetime_field = StringField(widget=AirflowDateTimePickerROWidget())

        form = TestForm()
        field = form.datetime_field

        html_output = field()

        assert 'readonly="true"' in html_output
        assert "input-group datetime datetimepicker" in html_output

    def test_bs3_text_field_ro_widget(self, app):
        class TestForm(FlaskForm):
            text_field = StringField(widget=BS3TextFieldROWidget())

        form = TestForm()
        field = form.text_field

        html_output = field()

        assert 'readonly="true"' in html_output
        assert "form-control" in html_output

    def test_bs3_text_area_ro_widget(self, app):
        class TestForm(FlaskForm):
            textarea_field = TextAreaField(widget=BS3TextAreaROWidget())

        form = TestForm()
        field = form.textarea_field

        html_output = field()

        assert 'readonly="true"' in html_output
        assert "form-control" in html_output


def is_db_isolation_mode():
    return os.environ.get("RUN_TESTS_WITH_DATABASE_ISOLATION", "false").lower() == "true"
