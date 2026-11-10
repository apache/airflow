#!/usr/bin/env python3
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
# /// script
# requires-python = ">=3.11"
# ///
"""
Check that the AIP and ADIP pages on the Airflow wiki are laid out the way their index pages expect.

The index pages are generated: each list is a Page Properties Report over the pages of one
folder, so a page in the wrong place, or with a State that does not match where it sits, shows
up in the wrong list or not at all. This script reads the wiki anonymously and reports every
such page.

AIP tree (https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals):

  * the index page has only the state folders (and the folder of ADIP stubs) as children;
  * every page in a state folder has one Page Properties block whose State badge matches the
    folder, the ``airflow-improvement-proposal`` label, and the state label of that folder only;
  * a page carrying a state label sits in that state's folder;
  * the counts in the titles of the folded sections on the index page match the folders.

ADIP tree: every child of the ADIP page is titled ``ADIP-<n> <title>`` with a unique number,
has the ``adip`` label and one Page Properties block with a State badge and a Formerly row.

Exits 1 when anything is out of line. With ``--slack-payload <file>`` it then also writes a
``chat.postMessage`` payload listing the problems, for the workflow to post.
"""

from __future__ import annotations

import argparse
import gzip
import http.client
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

WIKI = "https://cwiki.apache.org/confluence"
AIP_INDEX_ID = "89066602"
ADIP_INDEX_ID = "462816431"
ADIP_STUBS_FOLDER_ID = "462816661"
BASE_LABEL = "airflow-improvement-proposal"
ADIP_LABEL = "adip"
SLACK_CHANNEL = "internal-airflow-ci-cd"
MAX_PROBLEMS_IN_SLACK = 25
MAX_TITLE_LENGTH = 100
# Slack rejects a section block whose text is longer than 3000 characters.
MAX_SECTION_LENGTH = 2900
ATTEMPTS = 4


@dataclass(frozen=True)
class StateFolder:
    page_id: str
    label: str
    badge: str
    # Title of the folded section on the AIP index, with "{count}" where the count goes;
    # None when the section is not folded.
    folded_title: str | None = None


STATE_FOLDERS = (
    StateFolder("462816682", f"{BASE_LABEL}/wip", "WIP"),
    StateFolder("462816536", f"{BASE_LABEL}/draft", "Under discussion"),
    StateFolder("462816537", f"{BASE_LABEL}/accepted", "Accepted"),
    StateFolder(
        "173087665", f"{BASE_LABEL}/completed", "Completed", "{count} completed AIPs (click to expand)"
    ),
    StateFolder(
        "173087786",
        f"{BASE_LABEL}/abandoned",
        "Abandoned",
        "{count} abandoned or replaced AIPs (click to expand)",
    ),
)
STATE_LABELS = {folder.label for folder in STATE_FOLDERS}
ADIP_BADGES = {"Draft", "Accepted", "In progress", "Completed", "Abandoned"}

# Pages in a state folder that the people who keep the wiki in order cannot edit, with the
# reason. Their State badge and labels are not checked; their place in the tree still is.
RESTRICTED_PAGES = {
    "247827224": "AIP-52 has edit restrictions",
}


@dataclass
class Page:
    page_id: str
    title: str
    body: str = ""
    labels: list[str] = field(default_factory=list)

    @property
    def url(self) -> str:
        return f"{WIKI}/pages/viewpage.action?pageId={self.page_id}"


def get_json(path: str, params: dict[str, str], sleep: Callable[[float], None] = time.sleep) -> dict:
    url = f"{WIKI}/rest/api/{path}?{urllib.parse.urlencode(params)}"
    # Uncompressed responses of more than a few tens of kilobytes get cut off mid-transfer.
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "User-Agent": "apache-airflow-wiki-consistency-check",
        },
    )
    for attempt in range(1, ATTEMPTS):
        try:
            return read_json(request)
        except (http.client.HTTPException, OSError):
            sleep(5 * attempt)
    return read_json(request)


def read_json(request: urllib.request.Request) -> dict:
    with urllib.request.urlopen(request, timeout=60) as response:
        raw = response.read()
        if response.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
    return json.loads(raw)


def build_page(raw: dict) -> Page:
    return Page(
        page_id=raw["id"],
        title=raw["title"],
        body=raw.get("body", {}).get("storage", {}).get("value", ""),
        labels=[label["name"] for label in raw.get("metadata", {}).get("labels", {}).get("results", [])],
    )


def fetch_all(path: str, params: dict[str, str], fetch: Callable[[str, dict[str, str]], dict]) -> list[Page]:
    pages: list[Page] = []
    start = 0
    while True:
        data = fetch(path, {**params, "limit": "50", "start": str(start)})
        pages.extend(build_page(raw) for raw in data["results"])
        if not data["results"] or "next" not in data.get("_links", {}):
            return pages
        start += len(data["results"])


def fetch_page(page_id: str, fetch: Callable[[str, dict[str, str]], dict] = get_json) -> Page:
    return build_page(fetch(f"content/{page_id}", {"expand": "body.storage,metadata.labels"}))


def fetch_children(page_id: str, fetch: Callable[[str, dict[str, str]], dict] = get_json) -> list[Page]:
    # Bodies are fetched one page at a time: a response carrying several AIP bodies is large
    # enough to be cut off mid-transfer.
    listed = fetch_all(f"content/{page_id}/child/page", {}, fetch)
    return [fetch_page(page.page_id, fetch) for page in listed]


def fetch_labelled(label: str, fetch: Callable[[str, dict[str, str]], dict] = get_json) -> list[Page]:
    return fetch_all("content/search", {"cql": f'label = "{label}" and type = page'}, fetch)


def get_text(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html).replace("&nbsp;", " ")).strip()


def find_macro_body_end(body: str, start: int) -> int:
    """Return where the rich-text body opened after ``start`` closes, skipping nested macro bodies."""
    depth = 0
    for tag in re.finditer(r"<(/?)ac:rich-text-body>", body[start:]):
        depth += -1 if tag.group(1) else 1
        if depth == 0:
            return start + tag.start()
    return len(body)


def find_properties(body: str) -> list[dict[str, str]]:
    """Return the rows of each Page Properties block as {key: value storage HTML}."""
    blocks = []
    for match in re.finditer(r'<ac:structured-macro ac:name="details"', body):
        # Only a table inside the macro's own rich-text body is read by the index report; a
        # self-closing macro, or one with no body, has no properties.
        opening = re.match(
            r"[^>]*?(/?)>(?:\s*<ac:parameter[^>]*>.*?</ac:parameter>)*\s*<ac:rich-text-body>",
            body[match.end() :],
            re.S,
        )
        if not opening or opening.group(1):
            macro_body = ""
        else:
            body_start = match.end() + opening.end() - len("<ac:rich-text-body>")
            macro_body = body[body_start : find_macro_body_end(body, body_start)]
        table_end = macro_body.find("</table>")
        rows: dict[str, str] = {}
        for row in re.findall(r"<tr[^>]*>(.*?)</tr>", macro_body[: max(table_end, 0)], re.S):
            cells = re.findall(r"<t([dh])[^>]*>(.*?)</t\1>", row, re.S)
            if len(cells) >= 2:
                rows.setdefault(get_text(cells[0][1]).rstrip(":"), cells[1][1])
        blocks.append(rows)
    return blocks


def find_badge(cell: str) -> str | None:
    match = re.search(
        r'<ac:structured-macro ac:name="status".*?<ac:parameter ac:name="title">([^<]*)</ac:parameter>',
        cell,
        re.S,
    )
    return match.group(1) if match else None


def check_properties(page: Page, allowed_badges: set[str]) -> tuple[list[str], dict[str, str]]:
    blocks = find_properties(page.body)
    if len(blocks) != 1:
        return [f"has {len(blocks)} Page Properties blocks, expected 1"], blocks[0] if blocks else {}
    properties = blocks[0]
    if "State" not in properties:
        return ["has no State row in its Page Properties block"], properties
    badge = find_badge(properties["State"])
    if badge not in allowed_badges:
        return [f"State badge is {badge!r}, expected one of {sorted(allowed_badges)}"], properties
    return [], properties


def escape_for_slack(text: str) -> str:
    # Wiki titles are free text: unescaped, "<!channel>" would notify the channel and a "<" or
    # "|" would change the link.
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("|", "&#124;")


def shorten(title: str) -> str:
    return title if len(title) <= MAX_TITLE_LENGTH else title[: MAX_TITLE_LENGTH - 1] + "…"


def describe(page: Page, problem: str) -> str:
    return f"<{page.url}|{escape_for_slack(shorten(page.title))}>: {escape_for_slack(problem)}"


def check_aip_tree(
    index: Page,
    index_children: list[Page],
    folders: dict[str, list[Page]],
    labelled: dict[str, list[Page]],
) -> list[str]:
    problems = []
    allowed_children = {folder.page_id for folder in STATE_FOLDERS} | {ADIP_STUBS_FOLDER_ID}
    for page in index_children:
        if page.page_id not in allowed_children:
            problems.append(describe(page, "sits directly under the AIP index; move it into a state folder"))
    for missing in sorted(allowed_children - {page.page_id for page in index_children}):
        problems.append(
            describe(Page(missing, f"page {missing}"), "folder is no longer directly under the AIP index")
        )
    for folder in STATE_FOLDERS:
        pages = folders[folder.page_id]
        for page in pages:
            if page.page_id in RESTRICTED_PAGES:
                continue
            page_problems, _ = check_properties(page, {folder.badge})
            state_labels = STATE_LABELS.intersection(page.labels)
            if state_labels != {folder.label}:
                page_problems.append(f"state labels are {sorted(state_labels)}, expected ['{folder.label}']")
            if BASE_LABEL not in page.labels:
                page_problems.append(f"lacks the {BASE_LABEL!r} label")
            problems.extend(describe(page, problem) for problem in page_problems)
        in_folder = {page.page_id for page in pages}
        for page in labelled[folder.label]:
            if page.page_id not in in_folder:
                problems.append(
                    describe(page, f"has the {folder.label!r} label but is not in that state's folder")
                )
        if folder.folded_title:
            expected = folder.folded_title.format(count=len(pages))
            if f'<ac:parameter ac:name="title">{expected}</ac:parameter>' not in index.body:
                problems.append(describe(index, f"the folded section should be titled {expected!r}"))
    return problems


def check_adip_tree(children: list[Page]) -> list[str]:
    problems = []
    numbers: dict[int, Page] = {}
    for page in children:
        match = re.match(r"ADIP-(\d+) \S", page.title)
        if not match:
            problems.append(describe(page, "title does not start with 'ADIP-' and its number"))
        elif (number := int(match.group(1))) in numbers:
            problems.append(describe(page, f"reuses the number of {shorten(numbers[number].title)!r}"))
        else:
            numbers[number] = page
        if "(formerly" in page.title:
            problems.append(
                describe(page, "title repeats the former AIP number; it is the Formerly property")
            )
        if ADIP_LABEL not in page.labels:
            problems.append(describe(page, f"lacks the {ADIP_LABEL!r} label"))
        page_problems, properties = check_properties(page, ADIP_BADGES)
        if not page_problems and not re.fullmatch(r"AIP-\d+", get_text(properties.get("Formerly", ""))):
            page_problems.append(
                "has no Formerly row with the former AIP number in its Page Properties block"
            )
        problems.extend(describe(page, problem) for problem in page_problems)
    return problems


def collect_problems(fetch: Callable[[str, dict[str, str]], dict] = get_json) -> list[str]:
    index = fetch_page(AIP_INDEX_ID, fetch)
    folders = {folder.page_id: fetch_children(folder.page_id, fetch) for folder in STATE_FOLDERS}
    labelled = {folder.label: fetch_labelled(folder.label, fetch) for folder in STATE_FOLDERS}
    return check_aip_tree(index, fetch_children(AIP_INDEX_ID, fetch), folders, labelled) + check_adip_tree(
        fetch_children(ADIP_INDEX_ID, fetch)
    )


def build_slack_payload(problems: list[str], run_url: str | None) -> dict:
    shown = problems[:MAX_PROBLEMS_IN_SLACK]
    lines = [
        f":warning: *AIP/ADIP wiki consistency check found {len(problems)} problem(s)*",
        "Move each AIP page to the folder for its state and keep its State badge and labels in line "
        "with that folder.",
        *(f"• {problem}" for problem in shown),
    ]
    if len(problems) > len(shown):
        lines.append(f"… and {len(problems) - len(shown)} more")
    if run_url:
        lines.append(f"<{run_url}|View the run>")
    sections: list[str] = []
    for line in lines:
        if sections and len(sections[-1]) + 1 + len(line) <= MAX_SECTION_LENGTH:
            sections[-1] += "\n" + line
        else:
            sections.append(line)
    return {
        "channel": SLACK_CHANNEL,
        "text": f":warning: AIP/ADIP wiki consistency check found {len(problems)} problem(s)",
        "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": text}} for text in sections],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--slack-payload", type=Path, help="write a Slack payload here when problems are found"
    )
    parser.add_argument("--run-url", help="link to the CI run, added to the Slack message")
    args = parser.parse_args(argv)
    problems = collect_problems()
    if not problems:
        print("The AIP and ADIP wiki trees are consistent.")
        return 0
    print(f"Found {len(problems)} problem(s):")
    for problem in problems:
        print(f"  - {problem}")
    if args.slack_payload:
        args.slack_payload.write_text(json.dumps(build_slack_payload(problems, args.run_url), indent=2))
    return 1


if __name__ == "__main__":
    sys.exit(main())
