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
import { useEffect } from "react";

import { useDagVersionServiceGetDagVersionDiff } from "openapi/queries";

import { ProgressBar } from "src/system-components";

import { ErrorAlert } from "src/components/ErrorAlert";

import { VersionDiff } from "./VersionDiff";

type SelectedVersionsDiffProps = {
  readonly baseVersionNumber: number;
  readonly dagId: string;
  readonly maxChanges: number | undefined;
  /** Reports whether the bound cut this comparison short, for the field that sets it. */
  readonly onTruncatedChange: (truncated: boolean) => void;
  readonly targetVersionNumber: number;
};

/** Split out so the query only exists once both versions are chosen, rather than being disabled. */
export const SelectedVersionsDiff = ({
  baseVersionNumber,
  dagId,
  maxChanges,
  onTruncatedChange,
  targetVersionNumber,
}: SelectedVersionsDiffProps) => {
  const { data, error, isLoading } = useDagVersionServiceGetDagVersionDiff(
    {
      baseVersionNumber,
      dagId,
      maxChanges,
      targetVersionNumber,
    },
    undefined,
    // Changing the bound is a new query key; without this the table would blank out while it loads.
    { placeholderData: (previous) => previous },
  );

  const truncated = data?.truncated ?? false;

  // Cleared on the way out as well, so a bound that cut one comparison short does not go on
  // warning about the next pair of versions.
  useEffect(() => {
    onTruncatedChange(truncated);

    return () => onTruncatedChange(false);
  }, [onTruncatedChange, truncated]);

  return (
    <>
      <ErrorAlert error={error} />
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      {data === undefined ? undefined : (
        <VersionDiff
          baseVersionNumber={baseVersionNumber}
          diff={data}
          targetVersionNumber={targetVersionNumber}
        />
      )}
    </>
  );
};
