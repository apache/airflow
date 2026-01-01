#!/usr/bin/env python3
"""
Pre-commit hook: Ensure no Alembic migration script imports ORM models.
"""
import sys
import re
from pathlib import Path

MIGRATIONS_DIRS = [
    Path("airflow-core/src/airflow/migrations/versions"),
]
ORM_IMPORT_PATTERNS = [
    re.compile(r"from +airflow\\.models"),
    re.compile(r"import +airflow\\.models"),
    re.compile(r"from +airflow\\.models\\.", re.IGNORECASE),
    re.compile(r"import +airflow\\.models\\.", re.IGNORECASE),
]

def main():
    failed = False
    for migrations_dir in MIGRATIONS_DIRS:
        if not migrations_dir.exists():
            continue
        for pyfile in migrations_dir.glob("*.py"):
            with pyfile.open("r", encoding="utf-8") as f:
                for i, line in enumerate(f, 1):
                    for pat in ORM_IMPORT_PATTERNS:
                        if pat.search(line):
                            print(f"ORM import found in migration: {pyfile} (line {i}): {line.strip()}")
                            failed = True
    if failed:
        print("\nERROR: ORM model imports found in Alembic migration scripts. Use table reflection or raw SQL instead.")
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
