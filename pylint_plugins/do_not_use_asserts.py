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

from pylint.checkers import BaseChecker
from pylint.interfaces import IAstroidChecker
from pylint.lint import PyLinter


class DoNotUseAssertsChecker(BaseChecker):
    """
    The pylint plugin makes sure that no asserts are used in the main
    code of the application.
    """
    __implements__ = IAstroidChecker

    name = 'do-not-use-asserts'
    priority = -1
    msgs = {
        'E7401': (
            'Do not use asserts.',
            'do-not-use-asserts',
            'Asserts should not be used in the main Airflow code.'
        ),
    }

    def visit_assert(self, node):
        """
        Callback executed when assert is being visited by the parser.
        @param node: node where assert is being visited
        @return: None
        """
        self.add_message(
            self.name, node=node,
        )


def register(linter: PyLinter):
    """
    Registers the plugin.
    @param linter: linter to register the check
    @return: None
    """
    linter.register_checker(DoNotUseAssertsChecker(linter))
