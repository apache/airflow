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

from textwrap import dedent

import pytest
from check_first_limit import check_source, main


@pytest.mark.parametrize(
    "source",
    [
        "session.execute(select(Model).limit(1)).first()",
        "session.scalars(query.limit(1)).first()",
        "session.execute(statement=query.limit(1)).first()",
        "session.execute(query.limit(1)).scalars().unique().first()",
        "session.execute(query.limit(1)).scalars(0).first()",
        "session.execute(query.limit(1)).mappings().first()",
        "session.query(Model).limit(1).first()",
        "query = select(Model).limit(1)\nsession.scalars(query).first()",
        "query = select(Model)\nquery = query.limit(1)\nsession.execute(query).first()",
        "query: Select = select(Model).limit(1)\nsession.execute(query).first()",
        "query = select(Model).limit(1)\nother = query\nquery = select(Other)\nsession.execute(other).first()",
        "result = session.execute(query.limit(1))\nresult.first()",
        "(result := session.execute(query.limit(1))).first()",
        "session.execute(query.limit(1).where(Model.id == 1).order_by(Model.id)).first()",
        "session.execute(query.limit(1).options(joinedload(Model.other))).first()",
        "(await session.execute(query.limit(1))).first()",
        "(await session.stream_scalars(query.limit(1))).first()",
        "if condition:\n    query = select(Model).limit(1)\n    session.execute(query).first()",
        "query = select(Model).limit(1)\nif condition:\n    pass\nsession.execute(query).first()",
        "# session.execute(query).first()\ntext = 'result.first()'",
        "obj.first",
        "query: Select\nquery = select(Model).limit(1)\nsession.execute(query).first()",
    ],
)
def test_accepts_bounded_first(source: str):
    assert check_source(source) == []


@pytest.mark.parametrize(
    "source",
    [
        "session.execute(select(Model)).first()",
        "session.scalars(query).first()",
        "session.execute(statement=query).first()",
        "session.execute(query).scalars().unique().first()",
        "session.execute(query.limit(2)).first()",
        "session.execute(query.limit(True)).first()",
        "session.execute(query.limit(1.0)).first()",
        "session.execute(query.limit(count)).first()",
        "session.execute(query.limit(1).limit(None)).first()",
        "session.execute(query.limit(1).fetch(10)).first()",
        "session.execute(select(Model).where(Model.id.in_(select(Other.id).limit(1)))).first()",
        "session.execute(select(query.limit(1).subquery())).first()",
        "session.execute(query).limit(1).first()",
        "session.execute(query).limit(1).limit(1).first()",
        "query = select(Model).limit(1)\nquery = select(Other)\nsession.execute(query).first()",
        "query = select(Model)\nquery.limit(1)\nsession.execute(query).first()",
        "result = session.execute(query)\nquery = query.limit(1)\nresult.first()",
        "result = session.execute(query.limit(1))\nresult = session.execute(query)\nresult.first()",
        "session.execute(query).first()\nquery = query.limit(1)",
        "query = select(Model).limit(1)\ndel query\nsession.execute(query).first()",
        "query = select(Model).limit(1)\nquery += other\nsession.execute(query).first()",
        "query = select(Model).limit(1)\nquery, other = get_queries()\nsession.execute(query).first()",
        "if condition:\n    query = select(Model).limit(1)\nsession.execute(query).first()",
        "if condition:\n    query = select(Model).limit(1)\nelse:\n    session.execute(query).first()",
        "query = select(Model).limit(1)\nif condition:\n    query = select(Other)\nsession.execute(query).first()",
        "query = select(Model).limit(1)\nfor query in queries:\n    session.execute(query).first()",
        "query = select(Model).limit(1)\nwith context() as query:\n    session.execute(query).first()",
        "query = select(Model).limit(1)\ndef lookup(query):\n    return session.execute(query).first()",
        "def a():\n    query = select(Model).limit(1)\ndef b():\n    return session.execute(query).first()",
        "query = select(Model).limit(1)\nlookup = lambda query: session.execute(query).first()",
        "try:\n    query = select(Model).limit(1)\nexcept Error:\n    session.execute(query).first()",
        "(await session.execute(query)).first()",
        "session.execute(build_query(limit=1)).first()",
    ],
)
def test_rejects_unbounded_first(source: str):
    assert len(check_source(source)) == 1


def test_reports_all_locations():
    assert check_source("session.execute(query).first()\nresult.first()") == [
        (1, 1),
        (2, 1),
    ]


@pytest.mark.parametrize("invalid", [False, True])
def test_main(tmp_path, capsys, invalid):
    bounded = tmp_path / "bounded.py"
    bounded.write_text("session.execute(query.limit(1)).first()", encoding="utf-8")
    other = tmp_path / "other.py"
    other.write_text("session.execute(query).first()" if invalid else "pass", encoding="utf-8")

    assert main([str(bounded), str(other)]) == int(invalid)
    output = capsys.readouterr().out
    if invalid:
        assert f"{other}:1:1:" in output
        assert ".limit(1) on the statement before execution" in output
        assert str(bounded) not in output
    else:
        assert output == ""


def test_syntax_error_does_not_hide_other_failures(tmp_path, capsys):
    broken = tmp_path / "broken.py"
    broken.write_text("def broken(", encoding="utf-8")
    unbounded = tmp_path / "unbounded.py"
    unbounded.write_text("result.first()", encoding="utf-8")
    assert main([str(broken), str(unbounded)]) == 1
    output = capsys.readouterr().out
    assert f"{broken}:1:" in output
    assert f"{unbounded}:1:1:" in output


def test_multiline_first():
    source = dedent("""\
        def lookup():
            return session.execute(
                select(Model)
                .where(Model.id == 1)
            ).first()
    """)
    assert check_source(source) == [(2, 12)]
