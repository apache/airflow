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

import { ApiError, createApi } from "src/api";
import { type SessionResponse, isTerminalStatus } from "src/types/feedback";

const POLL_INTERVAL_MS = 3000;

interface UseSessionReturn {
  session: SessionResponse | null;
  error: string | null;
  loading: boolean;
  /** Whether the task is still running (undefined until first API response). */
  taskActive: boolean | undefined;
  sendFeedback: (text: string) => Promise<void>;
  approve: () => Promise<void>;
  reject: () => Promise<void>;
  /** Refetch session on demand. */
  refetch: () => Promise<void>;
}

export function useSession(
  dagId: string,
  runId: string,
  taskId: string,
  mapIndex: number,
): UseSessionReturn {
  const [session, setSession] = useState<SessionResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [taskActive, setTaskActive] = useState<boolean | undefined>(undefined);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const apiRef = useRef(createApi(dagId, runId, taskId, mapIndex));

  const stopPolling = useCallback(() => {
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
  }, []);

  const fetchSession = useCallback(async () => {
    try {
      const data = await apiRef.current.fetchSession();
      setSession(data);
      setError(null);
      setTaskActive(true);
      if (isTerminalStatus(data)) stopPolling();
    } catch (err) {
      if (err instanceof ApiError) {
        setError(err.message);
        if (err.taskActive !== undefined) {
          setTaskActive(err.taskActive);
          if (!err.taskActive) stopPolling();
        }
      } else {
        setError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      setLoading(false);
    }
  }, [stopPolling]);

  useEffect(() => {
    void fetchSession();
    timerRef.current = setInterval(() => void fetchSession(), POLL_INTERVAL_MS);
    return stopPolling;
  }, [fetchSession, stopPolling]);

  const sendFeedback = useCallback(
    async (text: string) => {
      let prevSession: SessionResponse | null = null;
      setSession((prev) => {
        prevSession = prev;
        if (!prev) return prev;
        return {
          ...prev,
          status: "changes_requested" as const,
          conversation: [
            ...prev.conversation,
            {
              role: "human" as const,
              content: text,
              iteration: prev.iteration,
            },
          ],
        };
      });
      try {
        const data = await apiRef.current.submitFeedback(text);
        setSession(data);
      } catch (err) {
        if (prevSession !== null) setSession(prevSession);
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [],
  );

  const approve = useCallback(async () => {
    try {
      const data = await apiRef.current.approve();
      setSession(data);
      stopPolling();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [stopPolling]);

  const reject = useCallback(async () => {
    try {
      const data = await apiRef.current.reject();
      setSession(data);
      stopPolling();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [stopPolling]);

  return { session, error, loading, taskActive, sendFeedback, approve, reject, refetch: fetchSession };
}
