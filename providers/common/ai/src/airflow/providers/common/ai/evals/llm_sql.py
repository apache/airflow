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

from dataclasses import dataclass
from typing import Any

import sqlglot
import sqlglot.expressions as exp
from pydantic_evals import Case
from pydantic_evals.evaluators import EvaluationReason, Evaluator, EvaluatorContext
from sqlglot import ErrorLevel

BLOCKED_EXPRESSION_MAPPER = {
    exp.Delete: "DELETE",
    exp.Drop: "DROP",
    exp.Grant: "GRANT",
    exp.Alter: "ALTER",
    exp.Transaction: "BEGIN",
    exp.Revoke: "REVOKE",
}


@dataclass
class ValidateSQL(Evaluator):
    """Validate generated sql."""

    BLOCKED_KEYWORDS: list[str] | None = None

    # TODO Identify and add more validations
    def blocked_key_word_validation(self, query: str) -> EvaluationReason | None:
        if not self.BLOCKED_KEYWORDS:
            return None

        results = []
        for statement in sqlglot.parse(query, error_level=sqlglot.ErrorLevel.RAISE):
            stmt_type = BLOCKED_EXPRESSION_MAPPER.get(type(statement), None)
            if stmt_type and stmt_type.upper() in self.BLOCKED_KEYWORDS:
                results.append("SQL contains blocked keyword: " + stmt_type)

        if results:
            return EvaluationReason(value=False, reason="\n".join(results))

        return None

    @staticmethod
    def sql_parser_validation(query: str) -> EvaluationReason | None:

        parsed = sqlglot.parse(query, error_level=ErrorLevel.RAISE)

        if not parsed:
            return EvaluationReason(
                value=False,
                reason="Could not parse SQL",
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
                reason="SQL is valid",
            )
        except Exception as e:
            return EvaluationReason(
                value=False,
                reason=f"Error while parsing SQL: {e}",
            )


def build_test_case(inputs: Any, case_name: str):
    return Case(name=case_name, inputs=inputs)
