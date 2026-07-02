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
import asyncio
import traceback
from collections import defaultdict
from collections.abc import Awaitable, Callable


class BaseDeleteHandler:
    # The function signature for the DELETERS's value: def f(resource: dict, prefix: str)
    DELETERS: dict[str, Callable[[dict, str], Awaitable[bool | None]]] = {}
    DELETION_ORDER: list[str] = []  # list of asset types to delete by order
    SEMAPHORE_COUNT: int = 10
    SLEEP_AFTER_EACH_REQUEST = 0.01

    def __init__(self):
        if not self.DELETERS or not self.DELETION_ORDER:
            raise NotImplementedError("To be able to use this handler you need to define DELETERS and DELETION_ORDER!")

    def categorize_resources(self, resources):
        categorized_resources = defaultdict(list)
        for resource in resources:
            asset_type = resource.get("assetType")
            key = asset_type if asset_type in self.DELETERS else "__UNKNOWN__"
            categorized_resources[key].append(resource)

        return categorized_resources

    async def delete_single_resource(
        self, resource: dict, counter: int, total_count: int, semaphore: asyncio.Semaphore
    ):
        async with semaphore:
            prefix = f"[{counter}/{total_count}] "
            asset_type = resource.get("assetType")
            if asset_type not in self.DELETERS:
                print(f"{prefix}Unknown asset type:", asset_type)
            else:
                deleter = self.DELETERS[asset_type]
                if await deleter(resource, prefix) is not False:
                    print(f"{prefix}Resource deleted!")

            await asyncio.sleep(self.SLEEP_AFTER_EACH_REQUEST)

    async def handle(self, resources: list[dict]):
        categorized_resources = self.categorize_resources(resources)

        semaphore = asyncio.Semaphore(self.SEMAPHORE_COUNT)

        for asset_type in self.DELETION_ORDER:
            if asset_type not in categorized_resources:
                print(f'Unknown asset type: "{asset_type}" in DELETION_ORDER! Passing...')
                continue

            resource_list = categorized_resources[asset_type]

            resource_count = len(resource_list)
            print(f"{asset_type} => {resource_count} resource(s) found! Deleting...")

            results = await asyncio.gather(
                *[
                    self.delete_single_resource(resource, idx, resource_count, semaphore)
                    for idx, resource in enumerate(resource_list, 1)
                ],
                return_exceptions=True,
            )

            # Expose errors silenced by asyncio.gather() call
            for result in results:
                if isinstance(result, Exception):
                    print("Found an error in the handle deletion results: ")
                    traceback.print_exception(type(result), result, result.__traceback__)
