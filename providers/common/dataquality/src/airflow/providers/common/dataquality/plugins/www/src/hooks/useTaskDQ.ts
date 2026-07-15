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

import { useCallback, useEffect, useState } from "react";

import { ApiError, createApi } from "src/api";
import type { RuleHistoryRecord, TaskDQRunRecord } from "src/types/dq";

interface UseTaskDQReturn {
  error: string | null;
  loading: boolean;
  loadMoreRuleHistory: () => void;
  loadMoreRuns: () => void;
  notFound: boolean;
  ruleHistory: Array<RuleHistoryRecord>;
  ruleHistoryError: string | null;
  ruleHistoryLoading: boolean;
  ruleHistoryLoadingMore: boolean;
  ruleHistoryNextCursor: string | null;
  runs: Array<TaskDQRunRecord>;
  runsLoadingMore: boolean;
  runsNextCursor: string | null;
}

export function useTaskDQ(
  dagId: string,
  taskId: string,
  selectedRuleUid: string | null,
  refreshKey = 0,
): UseTaskDQReturn {
  const [runs, setRuns] = useState<Array<TaskDQRunRecord>>([]);
  const [runsNextCursor, setRunsNextCursor] = useState<string | null>(null);
  const [runsLoadingMore, setRunsLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [loading, setLoading] = useState(true);
  const [ruleHistory, setRuleHistory] = useState<Array<RuleHistoryRecord>>([]);
  const [ruleHistoryNextCursor, setRuleHistoryNextCursor] = useState<string | null>(null);
  const [ruleHistoryLoadingMore, setRuleHistoryLoadingMore] = useState(false);
  const [ruleHistoryError, setRuleHistoryError] = useState<string | null>(null);
  const [ruleHistoryLoading, setRuleHistoryLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function loadRuns() {
      setLoading(true);
      try {
        const page = await createApi(dagId, taskId, "", -1).fetchTaskRuns();
        if (cancelled) return;
        setRuns(page.items);
        setRunsNextCursor(page.next_cursor);
        setNotFound(page.items.length === 0);
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

    void loadRuns();
    return () => {
      cancelled = true;
    };
  }, [dagId, refreshKey, taskId]);

  useEffect(() => {
    let cancelled = false;

    async function loadRuleHistory(ruleUid: string) {
      setRuleHistoryLoading(true);
      try {
        const page = await createApi(dagId, taskId, "", -1).fetchRuleHistory(ruleUid);
        if (cancelled) return;
        setRuleHistory(page.items);
        setRuleHistoryNextCursor(page.next_cursor);
        setRuleHistoryError(null);
      } catch (err) {
        if (cancelled) return;
        setRuleHistoryError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setRuleHistoryLoading(false);
      }
    }

    if (selectedRuleUid === null) {
      setRuleHistory([]);
      setRuleHistoryNextCursor(null);
      setRuleHistoryError(null);
      setRuleHistoryLoading(false);
      return undefined;
    }

    void loadRuleHistory(selectedRuleUid);
    return () => {
      cancelled = true;
    };
  }, [dagId, refreshKey, selectedRuleUid, taskId]);

  const loadMoreRuns = useCallback(() => {
    if (runsNextCursor === null || runsLoadingMore) return;

    setRunsLoadingMore(true);
    createApi(dagId, taskId, "", -1)
      .fetchTaskRuns({ before: runsNextCursor })
      .then((page) => {
        setRuns((current) => [...current, ...page.items]);
        setRunsNextCursor(page.next_cursor);
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => setRunsLoadingMore(false));
  }, [dagId, runsLoadingMore, runsNextCursor, taskId]);

  const loadMoreRuleHistory = useCallback(() => {
    if (selectedRuleUid === null || ruleHistoryNextCursor === null || ruleHistoryLoadingMore) return;

    setRuleHistoryLoadingMore(true);
    createApi(dagId, taskId, "", -1)
      .fetchRuleHistory(selectedRuleUid, { before: ruleHistoryNextCursor })
      .then((page) => {
        setRuleHistory((current) => [...current, ...page.items]);
        setRuleHistoryNextCursor(page.next_cursor);
      })
      .catch((err: unknown) => {
        setRuleHistoryError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => setRuleHistoryLoadingMore(false));
  }, [dagId, ruleHistoryLoadingMore, ruleHistoryNextCursor, selectedRuleUid, taskId]);

  return {
    error,
    loading,
    loadMoreRuleHistory,
    loadMoreRuns,
    notFound,
    ruleHistory,
    ruleHistoryError,
    ruleHistoryLoading,
    ruleHistoryLoadingMore,
    ruleHistoryNextCursor,
    runs,
    runsLoadingMore,
    runsNextCursor,
  };
}
