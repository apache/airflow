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

import gzip
import http.client
import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "verify_improvement_proposals_wiki.py"


@pytest.fixture
def wiki():
    module_name = "test_verify_improvement_proposals_wiki_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def badge(title, colour="Grey"):
    return (
        '<ac:structured-macro ac:name="status" ac:schema-version="1" ac:macro-id="x">'
        f'<ac:parameter ac:name="colour">{colour}</ac:parameter>'
        f'<ac:parameter ac:name="title">{title}</ac:parameter></ac:structured-macro>'
    )


def properties(rows):
    cells = "".join(
        f"<tr><td><strong>{key}</strong></td><td>{value}</td></tr>" for key, value in rows.items()
    )
    return (
        '<ac:structured-macro ac:name="details" ac:schema-version="1" ac:macro-id="y">'
        f"<ac:rich-text-body><table><tbody>{cells}</tbody></table></ac:rich-text-body></ac:structured-macro>"
    )


def folded(count, kind):
    return (
        '<ac:structured-macro ac:name="expand" ac:schema-version="1"><ac:parameter ac:name="title">'
        f"{count} {kind} AIPs (click to expand)</ac:parameter></ac:structured-macro>"
    )


def aip(wiki, folder, page_id="1", title="AIP-1 Something", state=None, labels=None):
    return wiki.Page(
        page_id,
        title,
        properties({"State": badge(state or folder.badge) + " extra detail"}),
        [wiki.BASE_LABEL, folder.label] if labels is None else labels,
    )


def build_tree(wiki, pages=None, index_children=None, labelled=None, index_body=None):
    """A consistent AIP tree with one AIP in each state folder, with overrides."""
    folders = {f.page_id: [aip(wiki, f, page_id=f"p{f.page_id}")] for f in wiki.STATE_FOLDERS}
    folders.update(pages or {})
    children = [wiki.Page(f.page_id, f.badge) for f in wiki.STATE_FOLDERS] + [
        wiki.Page(wiki.ADIP_STUBS_FOLDER_ID, "AIPs moved to ADIP")
    ]
    by_label = {f.label: folders[f.page_id] for f in wiki.STATE_FOLDERS}
    by_label.update(labelled or {})
    body = (
        index_body if index_body is not None else folded(1, "completed") + folded(1, "abandoned or replaced")
    )
    return (
        wiki.Page(wiki.AIP_INDEX_ID, "AIP index", body),
        children + (index_children or []),
        folders,
        by_label,
    )


def folder_of(wiki, badge_title):
    return next(f for f in wiki.STATE_FOLDERS if f.badge == badge_title)


def test_consistent_aip_tree_has_no_problems(wiki):
    assert wiki.check_aip_tree(*build_tree(wiki)) == []


def test_page_directly_under_the_aip_index_is_reported(wiki):
    stray = wiki.Page("99", "AIP-200 New")
    problems = wiki.check_aip_tree(*build_tree(wiki, index_children=[stray]))
    assert problems == [
        f"<{stray.url}|AIP-200 New>: sits directly under the AIP index; move it into a state folder"
    ]


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        pytest.param("", "has 0 Page Properties blocks, expected 1", id="no-block"),
        pytest.param(
            properties({"State": badge("Completed")}) * 2,
            "has 2 Page Properties blocks, expected 1",
            id="two-blocks",
        ),
        pytest.param(
            properties({"Discussion Thread": "x"}),
            "has no State row in its Page Properties block",
            id="no-state",
        ),
        pytest.param(
            properties({"State": "Completed"}),
            "State badge is None, expected one of ['Completed']",
            id="plain-text-state",
        ),
        pytest.param(
            properties({"State": badge("Accepted")}),
            "State badge is 'Accepted', expected one of ['Completed']",
            id="badge-of-another-state",
        ),
    ],
)
def test_aip_properties_must_match_the_folder(wiki, body, expected):
    completed = folder_of(wiki, "Completed")
    page = wiki.Page("5", "AIP-5 X", body, [wiki.BASE_LABEL, completed.label])
    problems = wiki.check_aip_tree(*build_tree(wiki, pages={completed.page_id: [page]}))
    assert problems == [f"<{page.url}|AIP-5 X>: {expected}"]


@pytest.mark.parametrize(
    ("labels", "expected"),
    [
        pytest.param(
            ["airflow-improvement-proposal"],
            ["state labels are [], expected ['airflow-improvement-proposal/completed']"],
            id="no-state-label",
        ),
        pytest.param(
            [
                "airflow-improvement-proposal",
                "airflow-improvement-proposal/completed",
                "airflow-improvement-proposal/draft",
            ],
            [
                "state labels are ['airflow-improvement-proposal/completed', 'airflow-improvement-proposal/draft'],"
                " expected ['airflow-improvement-proposal/completed']"
            ],
            id="two-state-labels",
        ),
        pytest.param(
            ["airflow-improvement-proposal/completed"],
            ["lacks the 'airflow-improvement-proposal' label"],
            id="no-base-label",
        ),
    ],
)
def test_aip_labels_must_match_the_folder(wiki, labels, expected):
    completed = folder_of(wiki, "Completed")
    page = aip(wiki, completed, page_id="5", title="AIP-5 X", labels=labels)
    problems = wiki.check_aip_tree(*build_tree(wiki, pages={completed.page_id: [page]}))
    assert problems == [f"<{page.url}|AIP-5 X>: {problem}" for problem in expected]


def test_restricted_page_is_not_checked_for_badge_or_labels(wiki):
    completed = folder_of(wiki, "Completed")
    page = wiki.Page(next(iter(wiki.RESTRICTED_PAGES)), "AIP-52 X", properties({"State": "Completed"}), [])
    assert wiki.check_aip_tree(*build_tree(wiki, pages={completed.page_id: [page]})) == []


def test_titles_and_problems_are_escaped_and_shortened_for_slack(wiki):
    page = wiki.Page("9", "AIP-9 <!channel> & a|b")
    assert (
        wiki.describe(page, "see <x>") == f"<{page.url}|AIP-9 &lt;!channel&gt; &amp; a&#124;b>: see &lt;x&gt;"
    )
    long_page = wiki.Page("10", "A" * 300)
    assert wiki.describe(long_page, "x") == f"<{long_page.url}|{'A' * 99}…>: x"


@pytest.mark.parametrize(
    "macro",
    [
        pytest.param('<ac:structured-macro ac:name="details" ac:schema-version="1" />', id="self-closing"),
        pytest.param(
            '<ac:structured-macro ac:name="details" ac:schema-version="1"></ac:structured-macro>',
            id="no-body",
        ),
    ],
)
def test_macro_without_a_body_has_no_properties_even_before_a_table(wiki, macro):
    body = macro + f"<table><tbody><tr><td>State</td><td>{badge('Completed')}</td></tr></tbody></table>"
    assert wiki.find_properties(body) == [{}]


def test_folder_moved_out_of_the_aip_index_is_reported(wiki):
    index, children, folders, labelled = build_tree(wiki)
    moved = children[0]
    problems = wiki.check_aip_tree(index, children[1:], folders, labelled)
    assert problems == [
        f"<{moved.url}|page {moved.page_id}>: folder is no longer directly under the AIP index"
    ]


def test_properties_with_a_nested_macro_in_a_cell_are_read(wiki):
    nested = (
        '<ac:structured-macro ac:name="info"><ac:rich-text-body><p>note</p></ac:rich-text-body>'
        "</ac:structured-macro>"
    )
    body = properties({"Discussion Thread": nested, "State": badge("Completed")})
    assert wiki.find_badge(wiki.find_properties(body)[0]["State"]) == "Completed"


def test_properties_table_outside_the_macro_is_not_read(wiki):
    body = (
        '<ac:structured-macro ac:name="details" ac:schema-version="1"><ac:rich-text-body><p></p>'
        "</ac:rich-text-body></ac:structured-macro>"
        f"<table><tbody><tr><td>State</td><td>{badge('Completed')}</td></tr></tbody></table>"
    )
    assert wiki.find_properties(body) == [{}]


def test_state_label_outside_its_folder_is_reported(wiki):
    draft = folder_of(wiki, "Under discussion")
    elsewhere = wiki.Page("77", "AIP-101: Appendix", labels=[draft.label])
    tree = build_tree(wiki)
    tree[3][draft.label] = [*tree[2][draft.page_id], elsewhere]
    problems = wiki.check_aip_tree(*tree)
    assert problems == [
        f"<{elsewhere.url}|AIP-101: Appendix>: has the '{draft.label}' label but is not in that state's folder"
    ]


def test_stale_count_in_a_folded_section_is_reported(wiki):
    tree = build_tree(wiki, index_body=folded(7, "completed") + folded(1, "abandoned or replaced"))
    problems = wiki.check_aip_tree(*tree)
    assert problems == [
        f"<{tree[0].url}|AIP index>: the folded section should be titled '1 completed AIPs (click to expand)'"
    ]


def adip(wiki, number=1, title=None, state="Completed", formerly="AIP-4", labels=("adip",)):
    rows = {"State": badge(state, "Green")}
    if formerly is not None:
        rows["Formerly"] = f'<span style="color: rgb(151,160,175);">{formerly}</span>'
    return wiki.Page(str(number), title or f"ADIP-{number} Something", properties(rows), list(labels))


def test_consistent_adip_tree_has_no_problems(wiki):
    assert wiki.check_adip_tree([adip(wiki, 1), adip(wiki, 2, formerly="AIP-6")]) == []


@pytest.mark.parametrize(
    ("page_kwargs", "expected"),
    [
        pytest.param(
            {"title": "Automation of tests"}, ["title does not start with 'ADIP-' and its number"], id="title"
        ),
        pytest.param(
            {"title": "ADIP-2 Something (formerly AIP-6)"},
            ["title repeats the former AIP number; it is the Formerly property"],
            id="formerly-in-title",
        ),
        pytest.param({"labels": ()}, ["lacks the 'adip' label"], id="no-label"),
        pytest.param(
            {"state": "Done"},
            [
                "State badge is 'Done', expected one of ['Abandoned', 'Accepted', 'Completed', 'Draft', 'In progress']"
            ],
            id="unknown-badge",
        ),
        pytest.param(
            {"formerly": None},
            ["has no Formerly row with the former AIP number in its Page Properties block"],
            id="no-formerly",
        ),
    ],
)
def test_adip_page_problems(wiki, page_kwargs, expected):
    page = adip(wiki, 2, **page_kwargs)
    assert wiki.check_adip_tree([adip(wiki, 1), page]) == [
        f"<{page.url}|{page.title}>: {p}" for p in expected
    ]


def test_duplicate_adip_number_is_reported_with_the_other_title_escaped(wiki):
    first, second = adip(wiki, 3, title="ADIP-3 <!channel>"), adip(wiki, 4, title="ADIP-3 Two")
    assert wiki.check_adip_tree([first, second]) == [
        f"<{second.url}|ADIP-3 Two>: reuses the number of 'ADIP-3 &lt;!channel&gt;'"
    ]


def test_fetch_children_pages_through_the_listing_and_reads_each_body(wiki):
    calls = []

    def fetch(path, params):
        calls.append((path, params.get("start")))
        if path.endswith("/child/page"):
            if params["start"] == "0":
                return {"results": [{"id": "1", "title": "A"}], "_links": {"next": "more"}}
            return {"results": [{"id": "2", "title": "B"}], "_links": {}}
        page_id = path.rsplit("/", 1)[1]
        return {
            "id": page_id,
            "title": f"T{page_id}",
            "body": {"storage": {"value": f"<p>{page_id}</p>"}},
            "metadata": {"labels": {"results": [{"name": "adip"}]}},
        }

    pages = wiki.fetch_children("10", fetch)
    assert [(p.page_id, p.title, p.body, p.labels) for p in pages] == [
        ("1", "T1", "<p>1</p>", ["adip"]),
        ("2", "T2", "<p>2</p>", ["adip"]),
    ]
    assert calls == [
        ("content/10/child/page", "0"),
        ("content/10/child/page", "1"),
        ("content/1", None),
        ("content/2", None),
    ]


def test_fetch_labelled_searches_by_label(wiki):
    fetch = mock.Mock(return_value={"results": [{"id": "3", "title": "AIP-3"}], "_links": {}})
    assert [p.page_id for p in wiki.fetch_labelled("airflow-improvement-proposal/wip", fetch)] == ["3"]
    assert fetch.call_args.args[1]["cql"] == 'label = "airflow-improvement-proposal/wip" and type = page'


@mock.patch("urllib.request.urlopen")
def test_get_json_retries_and_decompresses_gzip(urlopen, wiki):
    response = mock.MagicMock()
    response.__enter__.return_value = response
    response.read.return_value = gzip.compress(json.dumps({"ok": True}).encode())
    response.headers = {"Content-Encoding": "gzip"}
    urlopen.side_effect = [http.client.RemoteDisconnected("gone"), response]
    sleep = mock.Mock()
    assert wiki.get_json("content/1", {}, sleep=sleep) == {"ok": True}
    sleep.assert_called_once_with(5)
    assert urlopen.call_args.args[0].get_header("Accept-encoding") == "gzip"


@mock.patch("urllib.request.urlopen", side_effect=http.client.IncompleteRead(b""))
def test_get_json_gives_up_after_the_last_attempt(urlopen, wiki):
    with pytest.raises(http.client.IncompleteRead):
        wiki.get_json("content/1", {}, sleep=mock.Mock())
    assert urlopen.call_count == wiki.ATTEMPTS


def test_slack_payload_lists_problems_and_truncates(wiki):
    problems = [f"problem {i}" for i in range(wiki.MAX_PROBLEMS_IN_SLACK + 2)]
    payload = wiki.build_slack_payload(problems, "https://run")
    text = payload["blocks"][0]["text"]["text"]
    assert payload["channel"] == wiki.SLACK_CHANNEL
    assert f"found {len(problems)} problem(s)" in payload["text"]
    assert "• problem 0" in text
    assert f"• problem {wiki.MAX_PROBLEMS_IN_SLACK}" not in text
    assert "… and 2 more" in text
    assert text.endswith("<https://run|View the run>")


def test_main_exits_0_and_writes_no_payload_when_consistent(wiki, tmp_path):
    payload = tmp_path / "slack.json"
    with mock.patch.object(wiki, "collect_problems", return_value=[]):
        assert wiki.main(["--slack-payload", str(payload)]) == 0
    assert not payload.exists()


def test_main_exits_1_and_writes_the_payload_on_problems(wiki, tmp_path):
    payload = tmp_path / "slack.json"
    with mock.patch.object(wiki, "collect_problems", return_value=["bad page"]):
        assert wiki.main(["--slack-payload", str(payload), "--run-url", "https://run"]) == 1
    assert "• bad page" in json.loads(payload.read_text())["blocks"][0]["text"]["text"]


def test_slack_payload_splits_long_lists_into_sections_within_the_limit(wiki):
    problems = [f"problem {i} " + "x" * 400 for i in range(wiki.MAX_PROBLEMS_IN_SLACK)]
    sections = [block["text"]["text"] for block in wiki.build_slack_payload(problems, None)["blocks"]]
    assert len(sections) > 1
    assert all(len(text) <= wiki.MAX_SECTION_LENGTH for text in sections)
    assert "\n".join(sections).count("• problem") == wiki.MAX_PROBLEMS_IN_SLACK
