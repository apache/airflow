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
import { useEffect, useRef } from "react";
import { useLocalStorage } from "usehooks-ts";

type UseTaskStreamFilterProps = {
  readonly dagId: string;
  readonly taskId?: string;
};

type UseTaskStreamFilterReturn = {
  readonly includeDownstream: boolean;
  readonly includeUpstream: boolean;
  readonly setIncludeDownstream: (include: boolean) => void;
  readonly setIncludeUpstream: (include: boolean) => void;
  readonly setTaskStreamFilterRoot: (root: string | undefined) => void;
  readonly taskStreamFilterRoot: string | undefined;
};

/**
 * Custom hook to manage task stream filter state for the DAG views (Graph and Grid).
 *
 * This hook handles:
 * - Persisting filter state in localStorage
 * - Clearing filter on initial mount when no task is selected
 * - Managing filter state when tasks are clicked
 */
export const useTaskStreamFilter = ({
  dagId,
  taskId,
}: UseTaskStreamFilterProps): UseTaskStreamFilterReturn => {
  const [includeUpstream, setIncludeUpstream] = useLocalStorage<boolean>(
    `task_stream_filter_upstream-${dagId}`,
    false,
  );
  const [includeDownstream, setIncludeDownstream] = useLocalStorage<boolean>(
    `task_stream_filter_downstream-${dagId}`,
    false,
  );
  const [taskStreamFilterRoot, setTaskStreamFilterRoot] = useLocalStorage<string | undefined>(
    `task_stream_filter_root-${dagId}`,
    undefined,
  );

  const isInitialMountRef = useRef(true);

  useEffect(() => {
    if (isInitialMountRef.current) {
      isInitialMountRef.current = false;

      // If there's a filter saved in localStorage but no task is selected on mount,
      // clear the filter to prevent showing filtered view without context
      if (taskId === undefined && (includeUpstream || includeDownstream)) {
        setIncludeUpstream(false);
        setIncludeDownstream(false);
      }
    }
  }, [taskId, includeUpstream, includeDownstream, setIncludeUpstream, setIncludeDownstream]);

  return {
    includeDownstream,
    includeUpstream,
    setIncludeDownstream,
    setIncludeUpstream,
    setTaskStreamFilterRoot,
    taskStreamFilterRoot,
  };
};
