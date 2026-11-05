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

import { useDagRunServiceGetDagRun } from "openapi/queries";
import type { GridRunsResponse } from "openapi/requests/types.gen";

type Props = {
  readonly dagId: string;
  /** The latest page of Grid runs, without a date ceiling. */
  readonly initialGridRuns: ReadonlyArray<GridRunsResponse> | undefined;
  readonly noDateFilters: boolean;
  readonly runId: string | undefined;
  readonly setRunAfterLte: (value: string | undefined) => void;
};

/**
 * Move the Grid's window to a selected run that the latest page doesn't show, by writing the
 * run's `run_after` into the URL as the ceiling.
 *
 * It fires once per visit to a Dag's run levels: clicking between runs there never shifts the
 * window under the cursor. The detail layout stays mounted across levels and Dags, so the guard
 * is cleared at the Dag level and is scoped to the Dag it was set for; otherwise a second run
 * opened from the Runs tab or the run search would be left off the Grid.
 */
export const useSelectedRunCeiling = ({
  dagId,
  initialGridRuns,
  noDateFilters,
  runId,
  setRunAfterLte,
}: Props) => {
  const [ceilingSetFor, setCeilingSetFor] = useState<string | undefined>(undefined);

  if (runId === undefined && ceilingSetFor !== undefined) {
    setCeilingSetFor(undefined);
  }

  // Only true once initialGridRuns has loaded and lacks the selected run.
  const runMissingFromGrid =
    Boolean(runId) &&
    noDateFilters &&
    ceilingSetFor !== dagId &&
    initialGridRuns !== undefined &&
    !initialGridRuns.some((dr) => dr.run_id === runId);

  const { data: selectedDagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId: runId ?? "" }, undefined, {
    enabled: runMissingFromGrid,
  });

  // Both deps are needed: dag_run_id so we react when the fetch resolves,
  // runMissingFromGrid so we react when initialGridRuns loads and confirms
  // the run is absent (handles the case where selectedDagRun is already cached).
  useEffect(() => {
    if (selectedDagRun && runMissingFromGrid) {
      setCeilingSetFor(dagId);
      setRunAfterLte(selectedDagRun.run_after);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDagRun?.dag_run_id, runMissingFromGrid]);
};
