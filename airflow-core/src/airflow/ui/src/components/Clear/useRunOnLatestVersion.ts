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
 * 1. DAG-level configuration (highest)
 * 2. Global configuration
 * 3. System default (false)
 *
 * Uses nullable override pattern to avoid unnecessary re-renders.
 */
export const useRunOnLatestVersion = ({
  currentBundleVersion,
  dagLevelConfig,
  runBundleVersion,
}: UseRunOnLatestVersionProps): UseRunOnLatestVersionResult => {
  // Get global config value as string and convert correctly
  const globalConfigValue = useConfig("run_on_latest_version");
  const globalDefault = globalConfigValue === "true";

  // Calculate default based on precedence: DAG-level > Global > System default (false)
  // Use nullish coalescing for clean, efficient code
  const defaultValue = dagLevelConfig ?? globalDefault;

  // Use nullable override pattern: null until user interacts, then stores user's choice
  // This avoids state synchronization anti-pattern (useState + useEffect)
  const [userOverride, setUserOverride] = useState<boolean | null>(null);

  // Actual value: user's override if they've interacted, otherwise the default
  const value = userOverride ?? defaultValue;

  // Check if versions differ to determine if checkbox should be shown
  const bundleVersionsDiffer = currentBundleVersion !== runBundleVersion;
  const shouldShowCheckbox = bundleVersionsDiffer && runBundleVersion !== null && runBundleVersion !== "";

  return {
    setValue: setUserOverride,
    shouldShowCheckbox,
    value,
  };
};
