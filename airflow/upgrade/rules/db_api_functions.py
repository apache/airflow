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

from airflow.hooks.base_hook import BaseHook
from airflow.upgrade.rules.base_rule import BaseRule


def check_get_pandas_df(cls):
    try:
        cls.__new__(cls).get_pandas_df("fake SQL")
        return return_error_string(cls, "get_pandas_df")
    except NotImplementedError:
        pass
    except Exception:
        return return_error_string(cls, "get_pandas_df")


def check_run(cls):
    try:
        cls.__new__(cls).run("fake SQL")
        return return_error_string(cls, "run")
    except NotImplementedError:
        pass
    except Exception:
        return return_error_string(cls, "run")


def check_get_records(cls):
    try:
        cls.__new__(cls).get_records("fake SQL")
        return return_error_string(cls, "get_records")
    except NotImplementedError:
        pass
    except Exception:
        return return_error_string(cls, "get_records")


def return_error_string(cls, method):
    return (
        "Class {} incorrectly implements the function {} while inheriting from BaseHook. "
        "Please make this class inherit from airflow.hooks.db_api_hook.DbApiHook instead".format(
            cls, method
        )
    )


def get_all_non_dbapi_children():
    basehook_children = [
        child for child in BaseHook.__subclasses__() if child.__name__ != "DbApiHook"
    ]
    res = basehook_children[:]
    while basehook_children:
        next_generation = []
        for child in basehook_children:
            subclasses = child.__subclasses__()
            if subclasses:
                next_generation.extend(subclasses)
        res.extend(next_generation)
        basehook_children = next_generation
    return res


class DbApiRule(BaseRule):
    title = "Hooks that run DB functions must inherit from DBApiHook"

    description = (
        "Hooks that run DB functions must inherit from DBApiHook instead of BaseHook"
    )

    def check(self):
        basehook_subclasses = get_all_non_dbapi_children()
        incorrect_implementations = []
        for child in basehook_subclasses:
            pandas_df = check_get_pandas_df(child)
            if pandas_df:
                incorrect_implementations.append(pandas_df)
            run = check_run(child)
            if run:
                incorrect_implementations.append(run)
            get_records = check_get_records(child)
            if get_records:
                incorrect_implementations.append(get_records)
        return incorrect_implementations
