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
"""Example DAG demonstrating the usage DAG params to model a trigger UI with a user form.

This example DAG generates greetings to a list of provided names in selected languages in the logs.
"""

from __future__ import annotations

import datetime
from pathlib import Path

from airflow.sdk import DAG, Param, task
from airflow.utils.trigger_rule import TriggerRule

# [START params_trigger]
with DAG(
    dag_id=Path(__file__).stem,
    dag_display_name="Params Trigger UI",
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    schedule=None,
    start_date=datetime.datetime(2022, 3, 4),
    catchup=False,
    tags=["example", "params"],
    params={
        "names": Param(
            ["Linda", "Martha", "Thomas"],
            type="array",
            description="Define the list of names for which greetings should be generated in the logs."
            " Please have one name per line.",
            title="Names to greet",
        ),
        "english": Param(True, type="boolean", title="English"),
        "german": Param(True, type="boolean", title="German (Formal)"),
        "french": Param(True, type="boolean", title="French"),
    },
) as dag:

    @task(task_id="get_names", task_display_name="Get names")
    def get_names(**kwargs) -> list[str]:
        params = kwargs["params"]
        if "names" not in params:
            print("Uuups, no names given, was no UI used to trigger?")
            return []
        return params["names"]

    @task.branch(task_id="select_languages", task_display_name="Select languages")
    def select_languages(**kwargs) -> list[str]:
        params = kwargs["params"]
        selected_languages = []
        for lang in ["english", "german", "french"]:
            if params[lang]:
                selected_languages.append(f"generate_{lang}_greeting")
        return selected_languages

    @task(task_id="generate_english_greeting", task_display_name="Generate English greeting")
    def generate_english_greeting(name: str) -> str:
        return f"Hello {name}!"

    @task(task_id="generate_german_greeting", task_display_name="Erzeuge Deutsche Begrüßung")
    def generate_german_greeting(name: str) -> str:
        return f"Sehr geehrter Herr/Frau {name}."

    @task(task_id="generate_french_greeting", task_display_name="Produire un message d'accueil en français")
    def generate_french_greeting(name: str) -> str:
        return f"Bonjour {name}!"

    @task(task_id="print_greetings", task_display_name="Print greetings", trigger_rule=TriggerRule.ALL_DONE)
    def print_greetings(greetings1, greetings2, greetings3) -> None:
        for g in greetings1 or []:
            print(g)
        for g in greetings2 or []:
            print(g)
        for g in greetings3 or []:
            print(g)
        if not (greetings1 or greetings2 or greetings3):
            print("sad, nobody to greet :-(")

    lang_select = select_languages()
    names = get_names()
    english_greetings = generate_english_greeting.expand(name=names)
    german_greetings = generate_german_greeting.expand(name=names)
    french_greetings = generate_french_greeting.expand(name=names)
    lang_select >> [english_greetings, german_greetings, french_greetings]
    results_print = print_greetings(english_greetings, german_greetings, french_greetings)
# [END params_trigger]
