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

import string
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from googleapiclient.discovery import Resource

from airflow_breeze.utils.console import get_console

INTERESTING_OPSF_FIELDS = [
    "Score",
    "Code-Review",
    "Maintained",
    "Dangerous-Workflow",
    "Security-Policy",
    "Packaging",
    "Vulnerabilities",
]

INTERESTING_OPSF_SCORES = ["OPSF-" + field for field in INTERESTING_OPSF_FIELDS]
INTERESTING_OPSF_DETAILS = ["OPSF-Details-" + field for field in INTERESTING_OPSF_FIELDS]


class MetadataFromSpreadsheet(Enum):
    KNOWN_REPUTABLE_FOUNDATIONS = auto()
    KNOWN_STRONG_COMMUNITIES = auto()
    KNOWN_COMPANIES = auto()
    KNOWN_STABLE_PROJECTS = auto()
    KNOWN_LOW_IMPORTANCE_PROJECTS = auto()
    KNOWN_MEDIUM_IMPORTANCE_PROJECTS = auto()
    KNOWN_HIGH_IMPORTANCE_PROJECTS = auto()
    RELATIONSHIP_PROJECTS = auto()
    CONTACTED_PROJECTS = auto()


metadata_from_spreadsheet: dict[MetadataFromSpreadsheet, list[str]] = {}


def get_project_metadata(metadata_type: MetadataFromSpreadsheet) -> list[str]:
    return metadata_from_spreadsheet[metadata_type]


# This is a spreadsheet where we store metadata about projects that we want to use in our analysis
METADATA_SPREADSHEET_ID = "1Hg6_B_irfnqNltnu1OUmt7Ph-K6x-DTWF7GZ5t-G0iI"
# This is the named range where we keep metadata
METADATA_RANGE_NAME = "SpreadsheetMetadata"


def read_metadata_from_google_spreadsheet(sheets: Resource):
    get_console().print(
        "[info]Reading metadata from Google Spreadsheet: "
        f"https://docs.google.com/spreadsheets/d/{METADATA_SPREADSHEET_ID}"
    )
    range = sheets.values().get(spreadsheetId=METADATA_SPREADSHEET_ID, range=METADATA_RANGE_NAME).execute()
    metadata_types: list[MetadataFromSpreadsheet] = []
    for metadata_field in range["values"][0]:
        metadata_types.append(MetadataFromSpreadsheet[metadata_field])
        metadata_from_spreadsheet[MetadataFromSpreadsheet[metadata_field]] = []
    for row in range["values"][1:]:
        for index, value in enumerate(row):
            value = value.strip()
            if value:
                metadata_from_spreadsheet[metadata_types[index]].append(value)
    get_console().print("[success]Metadata read from Google Spreadsheet.")


def authorize_google_spreadsheets(json_credentials_file: Path, token_path: Path) -> Resource:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(token_path.as_posix(), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(json_credentials_file.as_posix(), SCOPES)
            creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
        token_path.write_text(creds.to_json())
    service = build("sheets", "v4", credentials=creds)
    sheets = service.spreadsheets()
    return sheets


def get_sheets(json_credentials_file: Path) -> Resource:
    token_path = Path.home() / ".config" / "gsheet" / "token.json"
    sheets = authorize_google_spreadsheets(json_credentials_file, token_path)
    return sheets


def write_sbom_information_to_google_spreadsheet(
    sheets: Resource,
    docs: dict[str, str],
    google_spreadsheet_id: str,
    all_dependencies: list[dict[str, Any]],
    fieldnames: list[str],
    include_opsf_scorecard: bool = False,
):
    # Use only interesting values from the scorecard
    cell_field_names = [
        fieldname
        for fieldname in fieldnames
        if fieldname in INTERESTING_OPSF_SCORES or not fieldname.startswith("OPSF-")
    ]

    num_rows = update_field_values(all_dependencies, cell_field_names, google_spreadsheet_id, sheets)
    if include_opsf_scorecard:
        get_console().print("[info]Updating OPSF detailed comments.")
        update_opsf_detailed_comments(
            all_dependencies, fieldnames, num_rows, google_spreadsheet_id, docs, sheets
        )


def update_opsf_detailed_comments(
    all_dependencies: list[dict[str, Any]],
    fieldnames: list[str],
    num_rows: int,
    google_spreadsheet_id: str,
    docs: dict[str, str],
    sheets: Resource,
):
    opsf_details_field_names = [
        fieldname for fieldname in fieldnames if fieldname in INTERESTING_OPSF_DETAILS
    ]
    start_opsf_column = fieldnames.index(opsf_details_field_names[0]) - 1
    opsf_details = []
    opsf_details.append(
        {
            "values": [
                {"note": docs[check]}
                for check in INTERESTING_OPSF_FIELDS
                if check != INTERESTING_OPSF_FIELDS[0]
            ]
        }
    )
    get_console().print("[info]Adding notes to all cells.")
    for dependency in all_dependencies:
        note_row = convert_sbom_dict_to_spreadsheet_data(opsf_details_field_names, dependency)
        opsf_details.append({"values": [{"note": note} for note in note_row]})
    notes = {
        "updateCells": {
            "range": {
                "startRowIndex": 1,
                "endRowIndex": num_rows + 1,
                "startColumnIndex": start_opsf_column,
                "endColumnIndex": start_opsf_column + len(opsf_details_field_names) + 1,
            },
            "rows": opsf_details,
            "fields": "note",
        },
    }
    update_note_body = {"requests": [notes]}
    get_console().print("[info]Updating notes in google spreadsheet.")
    sheets.batchUpdate(spreadsheetId=google_spreadsheet_id, body=update_note_body).execute()


def calculate_range(num_columns: int, row: int) -> str:
    # Generate column letters
    columns = list(string.ascii_uppercase)
    if num_columns > 26:
        columns += [f"{a}{b}" for a in string.ascii_uppercase for b in string.ascii_uppercase]

    # Calculate the range
    end_column = columns[num_columns - 1]
    return f"A{row}:{end_column}{row}"


def convert_sbom_dict_to_spreadsheet_data(headers: list[str], value_dict: dict[str, Any]):
    return [value_dict.get(header, "") for header in headers]


def update_field_values(
    all_dependencies: list[dict[str, Any]],
    cell_field_names: list[str],
    google_spreadsheet_id: str,
    sheets: Resource,
) -> int:
    get_console().print(f"[info]Updating {len(all_dependencies)} dependencies in the Google spreadsheet.")
    num_fields = len(cell_field_names)
    data = []
    top_header = []
    top_opsf_header_added = False
    top_actions_header_added = False
    possible_action_fields = [field[1] for field in ACTIONS.values()]
    for field in cell_field_names:
        if field.startswith("OPSF-") and not top_opsf_header_added:
            top_header.append("Relevant OPSF Scores and details")
            top_opsf_header_added = True
        elif field in possible_action_fields and not top_actions_header_added:
            top_header.append("Recommended actions")
            top_actions_header_added = True
        else:
            top_header.append("")

    simplified_cell_field_names = [simplify_field_names(field) for field in cell_field_names]
    get_console().print("[info]Adding top header.")
    data.append({"range": calculate_range(num_fields, 1), "values": [top_header]})
    get_console().print("[info]Adding second header.")
    data.append({"range": calculate_range(num_fields, 2), "values": [simplified_cell_field_names]})
    row = 3
    get_console().print("[info]Adding all rows.")
    for dependency in all_dependencies:
        spreadsheet_row = convert_sbom_dict_to_spreadsheet_data(cell_field_names, dependency)
        data.append({"range": calculate_range(num_fields, row), "values": [spreadsheet_row]})
        row += 1
    get_console().print("[info]Writing data.")
    body = {"valueInputOption": "RAW", "data": data}
    result = sheets.values().batchUpdate(spreadsheetId=google_spreadsheet_id, body=body).execute()
    get_console().print(
        f"[info]Updated {result.get('totalUpdatedCells')} cells values in the Google spreadsheet."
    )
    return row


def simplify_field_names(fieldname: str):
    if fieldname.startswith("OPSF-"):
        return fieldname[5:]
    return fieldname


ACTIONS: dict[str, tuple[int, str]] = {
    "Security-Policy": (9, "Add Security Policy to the repository"),
    "Vulnerabilities": (10, "Follow up with vulnerabilities"),
    "Packaging": (10, "Propose Trusted Publishing"),
    "Dangerous-Workflow": (10, "Follow up with dangerous workflow"),
    "Code-Review": (7, "Propose mandatory code review"),
}
