# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import sys
import time
import tracemalloc
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.context import ExecutionContext
from agent_skills_poc.resolver import resolve_command


def _build_skills_payload(workflow_count: int) -> dict[str, object]:
    skills: list[dict[str, object]] = []
    for idx in range(workflow_count):
        skills.append(
            {
                "id": f"skill_{idx}",
                "steps": [
                    {
                        "type": "local",
                        "command": f"uv run --project airflow-core pytest airflow-core/tests/test_{idx}.py -xvs",
                    },
                    {"type": "fallback", "command": "breeze run pytest airflow-core/tests -xvs"},
                ],
            }
        )
    return {"skills": skills}


def _mean_ms(callable_obj, iterations: int) -> float:
    start = time.perf_counter()
    for _ in range(iterations):
        callable_obj()
    elapsed = time.perf_counter() - start
    return (elapsed / iterations) * 1000.0


def run_resolver_benchmark(workflow_count: int, iterations: int = 200) -> tuple[float, float]:
    payload = _build_skills_payload(workflow_count)
    path = Path(__file__).resolve().parents[1] / "resolver_bench_skills.json"
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    target_skill_id = f"skill_{max(0, workflow_count - 1)}"
    context = ExecutionContext(environment="host")

    tracemalloc.start()
    try:
        resolver_ms = _mean_ms(
            lambda: resolve_command(target_skill_id, skills_path=path, context=context),
            iterations=iterations,
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        if path.exists():
            path.unlink()

    return resolver_ms, peak / (1024.0 * 1024.0)


def main() -> int:
    print("Running dedicated resolver-latency benchmark (real local measurements)\n")
    for count in [1, 10, 100]:
        resolver_ms, memory_mb = run_resolver_benchmark(workflow_count=count)
        print(f"Workflows: {count}")
        print(f"Resolver latency: {resolver_ms:.3f} ms")
        print(f"Memory usage: {memory_mb:.3f} MB")
        print("-")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
