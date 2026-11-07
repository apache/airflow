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
"""
Stand-in for the SAS binary in the end-to-end tests.

Called as ``sas_stub.py <data dir> <runs file> <SAS options> -sysin <program> -log <log>``:
it records the program, then copies the ``.sas7bdat`` the program's query reads
(``from <library>.<member>``) to the output library (``libname out '<dir>'``) as
``result.sas7bdat``, choosing the fixture written with the requested ``COMPRESS=``.
A statement without a result just succeeds; an unknown member fails as SAS does,
with an ERROR in the log and return code 2.
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

_OUT_LIBRARY = re.compile(r"libname out '((?:[^']|'')*)';")
_SOURCE_MEMBER = re.compile(r"\bfrom\s+\w+\.(\w+)", re.IGNORECASE)
_COMPRESS = re.compile(r"\(compress=(\w+)\)", re.IGNORECASE)
# The fixture file suffix written with each COMPRESS= value; YES is CHAR in SAS.
_VARIANTS = {"yes": "_char", "char": "_char", "binary": "_binary"}


def main(data_dir: Path, runs: Path, sas_args: list[str]) -> int:
    program = Path(sas_args[sas_args.index("-sysin") + 1]).read_text(errors="replace")
    log = Path(sas_args[sas_args.index("-log") + 1])
    with runs.open("a") as record:
        record.write(f"{program}\n<end of run>\n")

    out = _OUT_LIBRARY.search(program)
    if out is None:
        log.write_text("NOTE: PROCEDURE SQL used.\n")
        return 0
    member = _SOURCE_MEMBER.search(program).group(1).lower()  # type: ignore[union-attr]
    compress = _COMPRESS.search(program)
    variant = _VARIANTS.get(compress.group(1).lower(), "") if compress else ""
    candidates = [data_dir / f"{member}{variant}.sas7bdat", data_dir / f"{member}.sas7bdat"]
    source = next((path for path in candidates if path.exists()), None)
    if source is None:
        log.write_text(f"ERROR: File FX.{member.upper()}.DATA does not exist.\n")
        return 2
    shutil.copyfile(source, Path(out.group(1).replace("''", "'")) / "result.sas7bdat")
    log.write_text(f"NOTE: The data set OUT.RESULT has the rows of {source.name}.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3:]))
