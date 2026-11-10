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
"""Table search for :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

#: A table as the agent sees it: ``(qualified_name, comment)``. The name is exactly what
#: ``list_tables`` would return -- bare for the default schema, ``SCHEMA.TABLE`` otherwise --
#: so it round-trips to ``get_schema``.
TableEntry = tuple[str, str | None]


class SQLTableSearchBackend(Protocol):
    """
    Finds tables relevant to a free-text query, for the ``search_tables`` tool.

    The toolset filters whatever a backend returns through ``allowed_tables`` before the agent
    sees it, so a backend that knows about more tables than the agent may use cannot widen access.
    It can still waste its own ``limit``, so an index built for the agent should cover only the
    tables the agent may use.
    """

    def search_tables(
        self, query: str, limit: int, *, get_catalog: Callable[[], Sequence[TableEntry]]
    ) -> Sequence[TableEntry]:
        """
        Return up to ``limit`` tables, best match first.

        :param query: The agent's free-text query.
        :param limit: Maximum number of results wanted.
        :param get_catalog: Lists the tables the toolset exposes, introspected with the same
            connection the agent queries with and cached for the toolset's lifetime. A backend
            with its own index (a vector store, a search service) ignores it.
        """
        ...


class KeywordTableSearchBackend:
    """
    Default backend: case-insensitive substring match of each query word against table names and comments.

    Query words are split on whitespace. A table's score is the number of words found in its name
    weighted at ``_NAME_WEIGHT`` plus the number found in its comment; tables scoring zero are
    dropped, and ties are broken by name.
    """

    _NAME_WEIGHT = 3

    def search_tables(
        self, query: str, limit: int, *, get_catalog: Callable[[], Sequence[TableEntry]]
    ) -> list[TableEntry]:
        if not (words := query.casefold().split()):
            return []
        scored: list[tuple[int, str, TableEntry]] = []
        for entry in get_catalog():
            name, comment = entry
            name_text = name.casefold()
            comment_text = (comment or "").casefold()
            score = sum(self._NAME_WEIGHT * (word in name_text) + (word in comment_text) for word in words)
            if score:
                scored.append((score, name, entry))
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [entry for _, _, entry in scored[:limit]]
