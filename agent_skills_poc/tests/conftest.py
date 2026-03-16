# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import sys
from pathlib import Path

# Ensure tests can import the PoC package without editable installs.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
