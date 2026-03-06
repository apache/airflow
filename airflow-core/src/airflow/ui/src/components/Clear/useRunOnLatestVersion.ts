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

type UseRunOnLatestVersionProps = {
  /**
   * Current DAG bundle version
   */
  currentBundleVersion?: string | null;

  /**
   * DAG-level configuration from the DAG details.
   * If defined (true or false), takes precedence over global config.
   */
  dagLevelConfig?: boolean | null;

  /**
   * DAG run bundle version
   */
  runBundleVersion?: string | null;
};

type UseRunOnLatestVersionResult = {
  /**
   * Setter for the checkbox value
   */
  setValue: (value: boolean) => void;

  /**
   * Whether the checkbox should be shown
   */
  shouldShowCheckbox: boolean;

  /**
   * Current value of the checkbox (controlled)
   */
  value: boolean;
};

/**
 * Custom hook for managing "Run on Latest Version" checkbox state.
 *
 * Implements the three-level precedence hierarchy:
 * 1. DAG-level configuration (highest priority)
 * 2. Global configuration
 * 3. System default (false)
 *
 * Uses nullable override pattern: state is null until user interacts,
 * avoiding the useState + useEffect synchronization anti-pattern.
 */
export const useRunOnLatestVersion = ({
  currentBundleVersion,
  dagLevelConfig,
  runBundleVersion,
}: UseRunOnLatestVersionProps): UseRunOnLatestVersionResult => {
  const globalConfigValue = useConfig("run_on_latest_version");
  const globalDefault = Boolean(globalConfigValue);

  // Precedence: DAG-level > Global > System default (false)
  const defaultValue = dagLevelConfig ?? globalDefault;

  // Nullable override: null until user interacts, then stores their choice
  const [userOverride, setUserOverride] = useState<boolean | null>(null);
  const value = userOverride ?? defaultValue;

  // Show checkbox only when versions differ and run has a valid bundle version
  const hasValidRunVersion =
    runBundleVersion !== undefined && runBundleVersion !== null && runBundleVersion !== "";
  const shouldShowCheckbox = hasValidRunVersion && currentBundleVersion !== runBundleVersion;

  return {
    setValue: setUserOverride,
    shouldShowCheckbox,
    value,
  };
};
