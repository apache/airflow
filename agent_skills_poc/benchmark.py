# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import shutil
import tempfile
import time
import tracemalloc
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_skills_poc.generator import generate_agent_skills
from agent_skills_poc.parser import parse_workflows_from_text


def _build_rst(workflow_count: int) -> str:
    blocks: list[str] = []
    for idx in range(workflow_count):
        blocks.append(
            "\n".join(
                [
                    ".. agent-skill::",
                    f"   :id: run-tests-{idx}",
                    f"   :description: Run tests workflow {idx}",
                    f"   :local: uv run --project airflow-core pytest airflow-core/tests/test_{idx}.py -xvs",
                    "   :fallback: breeze run pytest airflow-core/tests -xvs",
                    "",
                ]
            )
        )
    return "PoC\n===\n\n" + "\n".join(blocks)


def _mean_ms(callable_obj, iterations: int) -> tuple[float, object]:
    result = None
    start = time.perf_counter()
    for _ in range(iterations):
        result = callable_obj()
    elapsed = time.perf_counter() - start
    return (elapsed / iterations) * 1000.0, result


def run_benchmark(workflow_count: int, iterations: int = 20) -> tuple[float, float, float]:
    rst_text = _build_rst(workflow_count)

    temp_root = Path(tempfile.mkdtemp(prefix="agent-skills-bench-"))
    try:
        tracemalloc.start()
        parse_ms, workflows = _mean_ms(
            lambda: parse_workflows_from_text(rst_text, source_path=f"<bench-{workflow_count}>"),
            iterations=iterations,
        )
        generate_ms, _ = _mean_ms(
            lambda: generate_agent_skills(workflows, temp_root / "skills"),
            iterations=iterations,
        )
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)

    return parse_ms, generate_ms, peak / (1024.0 * 1024.0)


def main() -> int:
    for count in [1, 10, 100]:
        parse_ms, generate_ms, memory_mb = run_benchmark(count)
        print(f"Workflows: {count}")
        print(f"Parsing: {parse_ms:.3f} ms")
        print(f"Generation: {generate_ms:.3f} ms")
        print(f"Memory: {memory_mb:.3f} MB")
        print("-")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
