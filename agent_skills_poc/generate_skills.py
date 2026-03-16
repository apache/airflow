# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_skills_poc.generator.generate_skills import main

if __name__ == "__main__":
    raise SystemExit(main())
