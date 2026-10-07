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

import { useCallback, useEffect, useRef, useState } from "react";

import { createModelApi } from "src/model-api";
import type { UsageInfo } from "src/types/model";

interface UseModelInfoReturn {
  modelName: string | null;
  usage: UsageInfo | null;
  loading: boolean;
  error: string | null;
  refetch: () => Promise<void>;
}

export function useModelInfo(
  dagId: string,
  runId: string,
  taskId: string,
  mapIndex: number,
): UseModelInfoReturn {
  const [modelName, setModelName] = useState<string | null>(null);
  const [usage, setUsage] = useState<UsageInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const apiRef = useRef(createModelApi(dagId, runId, taskId, mapIndex));

  const fetchInfo = useCallback(async () => {
    setLoading(true);
    try {
      const [name, usageData] = await Promise.all([
        apiRef.current.fetchModelName(),
        apiRef.current.fetchUsage(),
      ]);
      setModelName(name);
      setUsage(usageData);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void fetchInfo();
  }, [fetchInfo]);

  return { modelName, usage, loading, error, refetch: fetchInfo };
}
