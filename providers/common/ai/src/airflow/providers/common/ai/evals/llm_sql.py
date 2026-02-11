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

from dataclasses import dataclass
from typing import Any

from pydantic_evals import Case
from pydantic_evals.evaluators import EvaluationReason, Evaluator, EvaluatorContext


@dataclass
class ValidateSQL(Evaluator):
    """Validate generated sql."""

    BLOCKED_KEYWORDS: list[str] | None = None

    def blocked_key_word_validation(self, query: str) -> EvaluationReason | None:
        for key_word in self.BLOCKED_KEYWORDS:
            if key_word in query.upper():
                return EvaluationReason(value=False, reason=f'SQL contains blocked keyword: {key_word}')
        return None

    @staticmethod
    def sql_parser_validation(query: str) -> EvaluationReason | None:
        import sqlparse
        parsed = sqlparse.parse(query)

        if not parsed:
            return EvaluationReason(
                value=False,
                reason='Could not parse SQL',
            )

        return None

    def evaluate(self, ctx: EvaluatorContext) -> EvaluationReason:
        try:
            sql_parse_result = self.sql_parser_validation(ctx.output)
            blocked_keyword_result = self.blocked_key_word_validation(ctx.output)

            if sql_parse_result or blocked_keyword_result:
                return sql_parse_result or blocked_keyword_result

            return EvaluationReason(
                value=True,
                reason='SQL is valid',
            )
        except Exception as e:
            return EvaluationReason(
                value=False,
                reason=f'Error while parsing SQL: {e}',
            )

def build_test_case(inputs: Any, case_name: str):
    return Case(name=case_name, inputs=inputs)

