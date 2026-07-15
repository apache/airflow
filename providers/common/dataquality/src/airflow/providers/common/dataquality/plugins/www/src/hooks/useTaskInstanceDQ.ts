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

import { ApiError, createApi } from "src/api";
import type { TaskDQRunRecord } from "src/types/dq";

interface UseTaskInstanceDQReturn {
  error: string | null;
  loading: boolean;
  /** True when no data quality check has run yet for this task instance (HTTP 404). */
  notFound: boolean;
  result: TaskDQRunRecord | null;
}

export function useTaskInstanceDQ(
  dagId: string,
  taskId: string,
  runId: string,
  mapIndex: number,
): UseTaskInstanceDQReturn {
  const [result, setResult] = useState<TaskDQRunRecord | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const api = createApi(dagId, taskId, runId, mapIndex);

    async function load() {
      setLoading(true);
      try {
        const record = await api.fetchTaskInstanceRun();
        if (cancelled) return;
        setResult(record);
        setNotFound(false);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 404) {
          setNotFound(true);
          setError(null);
        } else {
          setError(err instanceof Error ? err.message : String(err));
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, [dagId, mapIndex, runId, taskId]);

  return { error, loading, notFound, result };
}
