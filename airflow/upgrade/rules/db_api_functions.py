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
from airflow.exceptions import AirflowException


def check_get_pandas_df(cls):
    try:
        cls.get_pandas_df(None, "fake SQL")
        return return_error_string(cls, "get_pandas_df")
    except NotImplementedError:
        pass
    except Exception as e:
        raise AirflowException(
            "the following hook incorrectly implements %s. error: %s", cls, e
        )


def check_run(cls):
    try:
        cls.run(None, "fake SQL")
        return return_error_string(cls, "run")
    except NotImplementedError:
        pass
    except Exception as e:
        raise AirflowException(
            "the following hook incorrectly implements run %s. error: %s", cls, e
        )


def check_get_records(cls):
    try:
        cls.get_records(None, "fake SQL")
        return return_error_string(cls, "get_records")
    except NotImplementedError:
        pass
    except Exception as e:
        raise AirflowException(
            "the following hook incorrectly implements run %s. error: %s", cls, e
        )


def return_error_string(cls, method):
    return (
        "Class {} incorrectly implements the function {} while inheriting from BaseHook. "
        "Please make this class inherit from airflow.hooks.db_api_hook.DbApiHook instead".format(
            cls, method
        )
    )


class DbApiRule(BaseRule):
    def check(self):
        subclasses = BaseHook.__subclasses__()
        incorrect_implementations = []
        for s in subclasses:
            if (
                "airflow.hooks" in s.__module__
                or "airflow.contrib.hooks.grpc_hook" in s.__module__
                or "tests.plugins.test_plugin.PluginHook" in str(s)
            ):
                pass
            else:
                pandas_df = check_get_pandas_df(s)
                if pandas_df:
                    incorrect_implementations.append(pandas_df)
                run = check_run(s)
                if run:
                    incorrect_implementations.append(run)
                get_records = check_get_records(s)
                if get_records:
                    incorrect_implementations.append(get_records)
        return incorrect_implementations
