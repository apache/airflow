/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { useEffect, useState } from "react";

import { createModelApi } from "src/model-api";
import { regionOf, type TaskRegion } from "src/region";
import type { UsageInfo } from "src/types/model";

interface UseModelInfoReturn {
  modelName: string | null;
  usage: UsageInfo | null;
  loading: boolean;
  error: string | null;
}

function describeFailure(result: PromiseSettledResult<unknown>): string | null {
  if (result.status === "fulfilled") return null;
  return result.reason instanceof Error ? result.reason.message : String(result.reason);
}

export function useModelInfo(
  dagId: string,
  runId: string,
  taskId: string,
  mapIndex: number,
  region?: TaskRegion,
): UseModelInfoReturn {
  const [modelName, setModelName] = useState<string | null>(null);
  const [usage, setUsage] = useState<UsageInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const regionId = region?.region_id;
  const regionIndex = region?.region_index;

  // Keyed on the region's fields, not the object, which the caller rebuilds on every render.
  // The host keeps the panel mounted when only the loop pass (a search param) changes, so a
  // read for the previous pass can still be in flight; its result is dropped.
  useEffect(() => {
    let current = true;
    setLoading(true);
    const api = createModelApi(
      dagId,
      runId,
      taskId,
      mapIndex,
      regionOf({ region_id: regionId, region_index: regionIndex }),
    );
    // allSettled, not all: one XCom missing or erroring must not hide a model name or usage
    // the other call did resolve.
    void Promise.allSettled([api.fetchModelName(), api.fetchUsage()]).then(([nameResult, usageResult]) => {
      if (!current) return;
      setModelName(nameResult.status === "fulfilled" ? nameResult.value : null);
      setUsage(usageResult.status === "fulfilled" ? usageResult.value : null);
      setError(describeFailure(nameResult) ?? describeFailure(usageResult));
      setLoading(false);
    });
    return () => {
      current = false;
    };
  }, [dagId, runId, taskId, mapIndex, regionId, regionIndex]);

  return { modelName, usage, loading, error };
}
