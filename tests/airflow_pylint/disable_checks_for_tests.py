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


from astroid import MANAGER, scoped_nodes
from pylint.lint import PyLinter

DISABLED_CHECKS_FOR_TESTS = \
    "missing-docstring, no-self-use, too-many-public-methods, protected-access, do-not-use-asserts"


def register(_: PyLinter):
    """
    Skip registering any plugin. This is not a real plugin - we only need it to register transform before
    running pylint.

    :param _:
    :return:
    """


def transform(mod):
    """
    It's a small hack but one that gives us a lot of speedup in pylint tests. We are replacing the first
    line of the file with pylint-disable (or update existing one) when file name start with `test_` or
    (for providers) when it is the full path of the package (both cases occur in pylint)

    :param mod: astroid module
    :return: None
    """
    if mod.name.startswith("test_") or \
            mod.name.startswith("tests.") or \
            mod.name.startswith("kubernetes_tests."):
        decoded_lines = mod.stream().read().decode("utf-8").split("\n")
        if decoded_lines[0].startswith("# pylint: disable="):
            decoded_lines[0] = decoded_lines[0] + " " + DISABLED_CHECKS_FOR_TESTS
        elif decoded_lines[0].startswith("#") or decoded_lines[0].strip() == "":
            decoded_lines[0] = "# pylint: disable=" + DISABLED_CHECKS_FOR_TESTS
        else:
            raise Exception(f"The first line of module {mod.name} is not a comment or empty. "
                            f"Please make sure it is!")
        # pylint will read from `.file_bytes` attribute later when tokenization
        mod.file_bytes = "\n".join(decoded_lines).encode("utf-8")


MANAGER.register_transform(scoped_nodes.Module, transform)
