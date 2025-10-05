from __future__ import annotations

import asyncio
import random

i: int = 0


async def task_runner(i):
    print(f"start task {i}")
    for _ in range(10):
        await asyncio.sleep(1)
        print(f"task {i} running")
    print(f"end task {i}")


async def fetch():
    if random.randint(0, 10) > 4:
        global i
        asyncio.create_task(task_runner(i))  # noqa: RUF006


async def main():
    global i
    print("start")
    while i < 100:
        await fetch()
        await asyncio.sleep(1)
        print(f"running {len(asyncio.all_tasks())}")
        i += 1
    while len(asyncio.all_tasks()) > 1:
        await asyncio.sleep(1)
        print(f"waiting for tasks to finish {len(asyncio.all_tasks())}")
    print("end")


asyncio.run(main())
