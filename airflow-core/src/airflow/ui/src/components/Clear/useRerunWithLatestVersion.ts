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
import { useState } from "react";

import { useConfig } from "src/queries/useConfig";

type UseRerunWithLatestVersionProps = {
  /** DAG-level rerun_with_latest_version from DAG details. */
  dagLevelConfig?: boolean | null;
  /**
   * Default to use when neither DAG-level nor global config is set.
   * Use `false` for clear/rerun (the historical default) and `true` for backfills.
   */
  fallback?: boolean;
};

type UseRerunWithLatestVersionResult = {
  setValue: (newValue: boolean) => void;
  value: boolean;
};

/**
 * Resolves the default checkbox state for "Run on Latest Version".
 * Precedence: user override > DAG-level > global config > fallback.
 *
 * The override is `undefined` until the user toggles the checkbox; once set, it
 * locks the value and won't be reset by config changes.
 */
export const useRerunWithLatestVersion = ({
  dagLevelConfig,
  fallback = false,
}: UseRerunWithLatestVersionProps): UseRerunWithLatestVersionResult => {
  const globalConfigValue = useConfig("rerun_with_latest_version") as boolean | undefined;
  const [override, setOverride] = useState<boolean | undefined>(undefined);

  const value = override ?? dagLevelConfig ?? globalConfigValue ?? fallback;

  return { setValue: setOverride, value };
};
